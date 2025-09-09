"""OAI-protocol based IE-import plugin."""

from typing import Optional, Mapping
from random import sample

import requests
from dcm_common.logger import LoggingContext, Logger
from dcm_common.plugins import PythonDependency, Signature, Argument, JSONType
from dcm_common.util import qjoin
from oai_pmh_extractor import (
    RepositoryInterface,
    PayloadCollector,
    TransferUrlFilters,
)

from dcm_import_module.models import IE
from .interface import IEImportPlugin, IEImportResult, IEImportContext


class OAIPMHPlugin(IEImportPlugin):
    """
    IE-import plugin based on the OAI-protocol [1].

    [1] https://www.openarchives.org/OAI/openarchivesprotocol.html
    """

    _DISPLAY_NAME = "OAI-PMH-Plugin"
    _NAME = "oai_pmh"
    _DESCRIPTION = (
        "IE import based on the Open Archives Initiative protocol for "
        + "metadata harvesting (OAI-PMH)."
    )
    _DEPENDENCIES = [
        PythonDependency("oai-pmh-extractor"),
        PythonDependency("requests"),
    ]

    _SIGNATURE = Signature(
        test=IEImportPlugin.signature.properties["test"],
        transfer_url_info=Argument(
            type_=JSONType.OBJECT,
            required=True,
            properties={
                "xml_path": Argument(
                    JSONType.ARRAY,
                    required=False,
                    item_type=JSONType.STRING,
                    description=(
                        "path in source metadata XML relative to 'record'; "
                        + "if omitted, the regex is applied to the entire "
                        + "source metadata XML"
                    ),
                    example=["metadata", "oai_dc:dc", "dc:identifier"],
                    default=None,
                ),
                "regex": Argument(
                    JSONType.STRING,
                    required=True,
                    description=(
                        "regex used to match content of 'xml_path' or full "
                        + "source metadata, respectively; based on "
                        + "'re.findall'"
                    ),
                    example=(
                        r"(https://repositorium\.uni-muenster\.de/transfer/.*)"
                    ),
                ),
            },
            description="information that allows to extract payload urls from "
            + "source metadata",
        ),
        base_url=Argument(
            JSONType.STRING,
            required=True,
            description="address for oai-pmh repository",
            example="https://repositorium.uni-muenster.de/oai/miami",
        ),
        metadata_prefix=Argument(
            type_=JSONType.STRING,
            required=True,
            description="metadata format",
            example="oai_dc",
        ),
        identifiers=Argument(
            type_=JSONType.ARRAY,
            required=False,
            item_type=JSONType.STRING,
            description=(
                "(optional) list of identifiers; if this is provided, "
                + "harvest settings are ignored"
            ),
            example=["oai:wwu.de:40871e80-89c0-42d0-a1a1-4a20056e7040"],
        ),
        from_=Argument(
            type_=JSONType.STRING,
            required=False,
            default=None,
            description="selective harvesting (lower bound of datestamps)",
            example="2023-01-01",
        ),
        until=Argument(
            type_=JSONType.STRING,
            required=False,
            default=None,
            description="selective harvesting (upper bound of datestamps)",
            example="2024-01-01",
        ),
        set_spec=Argument(
            type_=JSONType.STRING,
            required=False,
            default=None,
            description=(
                "selective harvesting (set membership as 'colon-separated "
                + "list indicating the path from the root of the set hierarchy"
                + " to the respective node')"
            ),
            example="doc-type:article",
        ),
    )

    @classmethod
    def _validate_more(cls, kwargs):
        schemes = ["http", "https"]
        if not any(
            kwargs["base_url"].startswith(scheme) for scheme in schemes
        ):
            return False, f"Bad url-scheme, supported are {qjoin(schemes)}"
        return True, ""

    def _get_collector(self, transfer_url_info: Mapping) -> PayloadCollector:
        if "xml_path" in transfer_url_info:
            return PayloadCollector(
                TransferUrlFilters.filter_by_regex_in_xml_path(
                    transfer_url_info["regex"],
                    transfer_url_info["xml_path"],
                ),
                self._timeout,
            )
        return PayloadCollector(
            TransferUrlFilters.filter_by_regex(transfer_url_info["regex"]),
            self._timeout,
        )

    def _list_identifiers(
        self,
        interface: RepositoryInterface,
        request_args: Mapping,
        /,
        **kwargs,
    ) -> tuple[Logger, Optional[list[str]]]:
        return self._retry(
            interface.list_identifiers_exhaustive,
            kwargs=request_args,
            description="collecting identifiers",
            exceptions=(requests.exceptions.ReadTimeout, OverflowError),
        )

    def _get_records(
        self,
        context: IEImportContext,
        interface: RepositoryInterface,
        collector: PayloadCollector,
        metadata_prefix: str,
        identifiers: list[str],
    ) -> None:
        """
        Downloads record metadata and payload for list of `identifiers`.
        Works on `result`.
        """

        for idx, identifier in enumerate(identifiers):
            # collect record
            context.set_progress(
                f"fetching metadata of record '{identifier}' "
                + f"({idx + 1}/{len(identifiers)})",
            )
            context.push()
            log, record = self._retry(
                interface.get_record,
                kwargs={
                    "metadata_prefix": metadata_prefix,
                    "identifier": identifier,
                },
                description=f"fetching metadata of record {identifier}",
                exceptions=requests.exceptions.ReadTimeout,
            )
            if LoggingContext.ERROR in log:
                context.result.log.merge(log)
                context.push()
                continue

            # process
            context.set_progress(
                f"processing metadata of record '{identifier}' "
                + f"({idx + 1}/{len(identifiers)})"
            )
            context.push()
            if record is None:
                context.result.log.log(
                    LoggingContext.ERROR, body=f"Got empty record '{identifier}'."
                )
                continue
            if record.status == "deleted":
                context.result.log.log(
                    LoggingContext.WARNING,
                    body=f"Record '{identifier}' has been marked 'deleted'.",
                )
                continue
            ie_path = self._get_ie_output()
            meta = ie_path / "meta"
            meta.mkdir()
            (meta / "source_metadata.xml").write_text(
                record.metadata_raw, encoding="utf-8"
            )
            payload = ie_path / "data" / "preservation_master"
            payload.mkdir(parents=True)

            # fetching payload
            context.set_progress(
                f"fetching payload of record '{identifier}' "
                + f"({idx + 1}/{len(identifiers)})"
            )
            context.push()
            log, _ = self._retry(
                collector.download_record_payload,
                args=(record, payload),
                description=f"fetching payload of record {identifier}",
                exceptions=requests.exceptions.ReadTimeout,
            )
            if LoggingContext.ERROR in log:
                context.result.log.merge(log)
                context.push()
                continue

            context.result.ies[
                "ie" + str(idx).zfill(len(str(len(identifiers))))
            ] = IE(
                path=ie_path,
                source_identifier=identifier,
                fetched_payload=all(
                    f["complete"] for f in record.files
                ),  # returns True when record.files = []
            )
            context.push()

        context.result.log.merge(
            Logger.octopus(
                interface.log, collector.log, default_origin=self._NAME
            )
        )
        context.push()

    def _get(self, context: IEImportContext, /, **kwargs) -> IEImportResult:
        context.set_progress(
            f"plugin '{self._NAME}' initializing job",
        )
        context.push()

        # initialize from request-args
        interface = RepositoryInterface(kwargs["base_url"], self._timeout)
        interface.preserve_log = True
        collector = self._get_collector(kwargs["transfer_url_info"])

        # fetch identifiers
        if "identifiers" in kwargs:
            # 'identifiers' takes priority over selective harvesting
            identifiers = kwargs["identifiers"]
            if not isinstance(identifiers, list):
                identifiers = [identifiers]
        else:
            context.set_progress(
                f"collecting identifiers from '{kwargs['base_url']}'"
            )
            context.push()
            # get identifiers via interface
            request_args = {  # build request body
                "metadata_prefix": kwargs["metadata_prefix"],
                "_max_resumption_tokens": self._max_resumption_tokens
            }
            if "from_" in kwargs:
                request_args["_from"] = kwargs["from_"]
            if "until" in kwargs:
                request_args["_until"] = kwargs["until"]
            if "set_spec" in kwargs:
                request_args["_set_spec"] = kwargs["set_spec"]

            log, identifiers = self._list_identifiers(
                interface, request_args, **kwargs
            )
            context.result.log.merge(log)
            if LoggingContext.ERROR in log:
                context.set_progress("collecting identifiers failed")
                context.result.log.log(
                    LoggingContext.ERROR, body="Collecting identifiers failed."
                )
                context.push()
                return context.result

            # test-import: limit number of identifiers
            if (
                kwargs.get("test", False)
                and identifiers is not None
                and len(identifiers) > self._test_volume >= 0
            ):
                context.result.log.log(
                    LoggingContext.INFO,
                    body=(
                        "Limiting number of identifiers from "
                        + f"{len(identifiers)} down to {self._test_volume} "
                        + f"via '{self._test_strategy}'-strategy."
                    ),
                )
                context.push()
                match self._test_strategy:
                    case "first":
                        identifiers = identifiers[: self._test_volume]
                    case "random":
                        identifiers = sample(identifiers, k=self._test_volume)
                    case _:
                        raise ValueError(
                            f"Unknown test-strategy '{self._test_strategy}'."
                        )

        # process identifiers
        if len(identifiers) == 0:
            context.result.log.log(
                LoggingContext.WARNING, body="List of identifiers is empty."
            )
            context.push()
        else:
            # download records
            self._get_records(
                context,
                interface,
                collector,
                kwargs["metadata_prefix"],
                identifiers,
            )

        context.result.success = all(
            ie.fetched_payload for ie in context.result.ies.values()
        )
        if context.result.success:
            context.set_progress("success")
        else:
            context.set_progress("failure")
        context.push()
        return context.result


class OAIPMHPlugin2(OAIPMHPlugin):
    """
    IE-import plugin based on the OAI-protocol [1].

    It inherits from the `OAIPMHPlugin` and adds support for multiple sets,
    and multiple transferUrlFilters based on xpath.

    [1] https://www.openarchives.org/OAI/openarchivesprotocol.html
    """

    _NAME = "oai_pmh_v2"
    _DESCRIPTION = (
        "IE import based on the Open Archives Initiative protocol for "
        + "metadata harvesting (OAI-PMH). "
        + "Supports multiple sets and transferUrlFilters."
    )
    _SIGNATURE = Signature(
        test=IEImportPlugin.signature.properties["test"],
        base_url=OAIPMHPlugin.signature.properties["base_url"],
        metadata_prefix=OAIPMHPlugin.signature.properties["metadata_prefix"],
        identifiers=OAIPMHPlugin.signature.properties["identifiers"],
        from_=OAIPMHPlugin.signature.properties["from_"],
        until=OAIPMHPlugin.signature.properties["until"],
        transfer_url_info=Argument(
            type_=JSONType.ARRAY,
            required=True,
            item_type=Argument(
                type_=JSONType.OBJECT,
                required=True,
                description=(
                    "information that allows to extract payload urls from "
                    + "source metadata"
                ),
                properties={
                    "path": Argument(
                        JSONType.STRING,
                        required=False,
                        item_type=JSONType.STRING,
                        description=(
                            "xpath query; "
                            + "if omitted, the regex is applied to the entire "
                            + "source metadata XML"
                        ),
                        example=(
                            "./GetRecord/record/metadata/"
                            + "oai_dc:dc/dc:identifier"
                        ),
                        default=None,
                    ),
                    "regex": Argument(
                        JSONType.STRING,
                        required=True,
                        description=(
                            "regex used to match content of 'path' or "
                            + "full source metadata; based on 're.findall'"
                        ),
                        example=(
                            r"(https://repositorium\.uni-muenster\.de/transfer/.*)"
                        ),
                    ),
                },
            ),
        ),
        set_spec=Argument(
            type_=JSONType.ARRAY,
            required=False,
            default=None,
            item_type=JSONType.STRING,
            description=(
                "selective harvesting (list of set memberships; "
                + "identifiers combined via a logical OR-operation)"
            ),
            example=["doc-type:article"],
        ),
    )

    def _get_collector(self, transfer_url_info: Mapping) -> PayloadCollector:
        return PayloadCollector(
            transfer_url_filters=list(
                map(
                    lambda info: (
                        TransferUrlFilters.filter_by_regex_with_xpath_query(
                            **info
                        )
                        if "path" in info
                        else TransferUrlFilters.filter_by_regex(**info)
                    ),
                    transfer_url_info,
                )
            ),
            timeout=self._timeout
        )

    def _list_identifiers(
        self,
        interface: RepositoryInterface,
        request_args: Mapping,
        /,
        **kwargs,
    ) -> tuple[Logger, Optional[list[str]]]:
        return self._retry(
            interface.list_identifiers_exhaustive_multiple_sets,
            kwargs=request_args,
            description="collecting identifiers",
            exceptions=(requests.exceptions.ReadTimeout, OverflowError),
        )
