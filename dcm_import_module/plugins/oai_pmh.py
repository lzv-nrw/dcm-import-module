"""
This module defines the plugin-class for importing IEs based on the OAI-
protocol in the 'DCM Import Module'.
"""

import requests
from data_plumber_http import Url
from data_plumber_http.settings import Responses
from oai_pmh_extractor \
    import RepositoryInterface, PayloadCollector, TransferUrlFilters
from dcm_common import LoggingContext as Context, Logger
from dcm_common.models.report import Status

from dcm_import_module.plugins import Interface
from dcm_import_module.models import JSONType, Signature, Argument, IE, \
    PluginResult


class OAIPMH(Interface):
    _NAME = "oai_pmh"
    _DESCRIPTION = "Plugin for import based on the OAI-protocol for metadata" \
        + " harvesting."
    _DEPENDENCIES = [
        "oai-pmh-extractor",
        "dcm-common",
    ]
    _SIGNATURE = Signature(
        transfer_url_info=Argument(
            type_=JSONType.OBJECT,
            required=True,
            properties={
                "xml_path": Argument(
                    JSONType.ARRAY,
                    required=False,
                    item_type=JSONType.STRING,
                    description="path in source metadata XML relative to "
                        + "'record'; if omitted, the regex is applied to the "
                        + "entire source metadata XML",
                    example=["metadata", "oai_dc:dc", "dc:identifier"],
                    default=None
                ),
                "regex": Argument(
                    JSONType.STRING,
                    required=True,
                    description="regex used to match content of 'xml_path' or "
                        + "full source metadata, respectively; based on "
                        + "'re.findall'",
                    example=r"(https://repositorium\.uni-muenster\.de/transfer/.*)"
                ),
            },
            description="information that allows to extract payload urls from "
                + "source metadata"
        ),
        base_url=Argument(
            JSONType.STRING, required=True,
            description="address for oai-pmh repository",
            example="https://repositorium.uni-muenster.de/oai/miami"
        ),
        metadata_prefix=Argument(
            type_=JSONType.STRING, required=True,
            description="metadata format",
            example="oai_dc"
        ),
        identifiers=Argument(
            type_=JSONType.ARRAY, required=False, item_type=JSONType.STRING,
            description="(optional) list of identifiers; if this is provided, "
                + "harvest settings are ignored",
            example=["oai:wwu.de:40871e80-89c0-42d0-a1a1-4a20056e7040"]
        ),
        from_=Argument(
            type_=JSONType.STRING, required=False, default=None,
            description="selective harvesting (lower bound of datestamps)",
            example="2023-01-01"
        ),
        until=Argument(
            type_=JSONType.STRING, required=False, default=None,
            description="selective harvesting (upper bound of datestamps)",
            example="2024-01-01"
        ),
        set_spec=Argument(
            type_=JSONType.STRING, required=False, default=None,
            description="selective harvesting (set membership as 'colon-"
                + "separated list indicating the path from the root of the set"
                + " hierarchy to the respective node')",
            example="doc-type:article"
        ),
    )

    @classmethod
    def _validate_more(cls, **kwargs) -> tuple[bool, str]:
        # validate url
        output = Url(schemes=["http", "https"]).make(
            kwargs["base_url"], "import.args"
        )
        if output[2] != Responses.GOOD.status:
            return False, output[1]
        return True, ""

    def _get_records(
        self,
        result: PluginResult,
        interface,  # RepositoryInterface
        collector,  # PayloadCollector
        metadata_prefix: str,
        identifiers: list[str]
    ) -> None:
        """
        Downloads record metadata and payload for list of `identifiers`.
        Works on `result`.
        """

        for idx, identifier in enumerate(identifiers):
            self.set_progress(
                verbose=f"fetching metadata of record '{identifier}' "
                + f"({idx + 1}/{len(identifiers)})",
                numeric=int(100*idx/len(identifiers))
            )
            log, record = self._retry(
                interface.get_record,
                kwargs={
                    "metadata_prefix": metadata_prefix,
                    "identifier": identifier
                },
                description=f"fetching metadata of record {identifier}",
                exceptions=requests.exceptions.ReadTimeout
            )
            if Context.ERROR in log:
                result.log.merge(log)
                continue
            self.set_progress(
                verbose=f"processing metadata of record '{identifier}' "
                + f"({idx + 1}/{len(identifiers)})"
            )

            # handle errors
            if record is None:
                result.log.log(
                    Context.ERROR,
                    body=f"Got empty record '{identifier}'."
                )
                continue
            if record.status == "deleted":
                result.log.log(
                    Context.WARNING,
                    body=f"Record '{identifier}' has been marked 'deleted'."
                )
                continue

            ie_path = self._get_ie_output()
            meta = ie_path / "meta"
            meta.mkdir()
            (meta / "source_metadata.xml").write_text(
                record.metadata_raw,
                encoding="utf-8"
            )
            payload = ie_path / "data" / "preservation_master"
            payload.mkdir(parents=True)
            self.set_progress(
                verbose=f"fetching payload of record '{identifier}' "
                + f"({idx + 1}/{len(identifiers)})"
            )
            log, _ = self._retry(
                collector.download_record_payload,
                args=(record, payload),
                description=f"fetching payload of record {identifier}",
                exceptions=requests.exceptions.ReadTimeout
            )
            if Context.ERROR in log:
                result.log.merge(log)
                continue

            result.ies[
                "ie" + str(idx).zfill(len(str(len(identifiers))))
            ] = \
                IE(
                    path=ie_path,
                    source_identifier=identifier,
                    fetched_payload=all(
                        f["complete"] for f in record.files
                    )  # returns True when record.files = []
                )

        result.log.merge(
            Logger.octopus(
                interface.log,
                collector.log,
                default_origin=self._NAME
            )
        )

    def get(self, **kwargs) -> PluginResult:
        result = PluginResult(log=Logger(default_origin=self._NAME))
        self.set_progress(
            verbose=f"plugin '{self._NAME}' starting up",
            numeric=0, status=Status.RUNNING
        )

        interface = RepositoryInterface(kwargs["base_url"], self._timeout)
        interface.preserve_log = True
        if "xml_path" in kwargs["transfer_url_info"]:
            collector = PayloadCollector(
                TransferUrlFilters.filter_by_regex_in_xml_path(
                    kwargs["transfer_url_info"]["regex"],
                    kwargs["transfer_url_info"]["xml_path"]
                ),
                self._timeout
            )
        else:
            collector = PayloadCollector(
                TransferUrlFilters.filter_by_regex(
                    kwargs["transfer_url_info"]["regex"]
                ),
                self._timeout
            )

        if "identifiers" in kwargs:
            # 'identifiers' takes priority over selective harvesting
            identifiers = kwargs["identifiers"]
            if not isinstance(identifiers, list):
                identifiers = [identifiers]
        else:
            self.set_progress(
                verbose=f"collecting identifiers from '{kwargs['base_url']}'"
            )
            # get identifiers via interface
            request_args = {  # build request body
                "metadata_prefix": kwargs["metadata_prefix"],
            }
            if "from_" in kwargs:
                request_args["_from"] = kwargs["from_"]
            if "until" in kwargs:
                request_args["_until"] = kwargs["until"]
            if "set_spec" in kwargs:
                request_args["_set_spec"] = kwargs["set_spec"]

            log, identifiers = self._retry(
                interface.list_identifiers_exhaustive,
                kwargs=request_args,
                description="collecting identifiers",
                exceptions=requests.exceptions.ReadTimeout
            )
            if Context.ERROR in log:
                self.set_progress(
                    verbose="collecting identifiers failed",
                    numeric=100,
                    status=Status.COMPLETED
                )
                result.log.merge(log)
                result.log.log(
                    Context.ERROR,
                    body="Collecting identifiers failed."
                )
                return result

        if len(identifiers) == 0:
            result.log.log(
                Context.WARNING,
                body="List of identifiers is empty."
            )
            result.log.merge(interface.log)
        else:
            # download records
            self._get_records(
                result,
                interface,
                collector,
                kwargs["metadata_prefix"],
                identifiers
            )

        self.set_progress(
            verbose="all records processed",
            numeric=100,
            status=Status.COMPLETED
        )
        return result
