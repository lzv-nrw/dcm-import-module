"""
Import View-class definition
"""

from typing import Optional
from pathlib import Path
from time import sleep
import json
import urllib3

from flask import Blueprint, Response, jsonify
from pydantic import ValidationError
from data_plumber_http.decorators import flask_handler, flask_args, flask_json
from dcm_common import LoggingContext as Context
from dcm_common.models import JSONable
from dcm_common.models.report import Report as BaseReport
from dcm_common.orchestration import JobConfig, Job, Children, ChildJobEx
from dcm_common import services
import dcm_ip_builder_sdk

from dcm_import_module.config import AppConfig
from dcm_import_module.models import ImportConfigExternal, IP
from dcm_import_module.handlers import get_external_import_handler


class ExternalImportView(services.OrchestratedView):
    """View-class for ip-import from external system."""
    NAME = "external-import"

    def __init__(
        self, config: AppConfig, *args, **kwargs
    ) -> None:
        super().__init__(config, *args, **kwargs)

        # ip-builder sdk
        self.ip_builder_api = dcm_ip_builder_sdk.BuildApi(
            dcm_ip_builder_sdk.ApiClient(
                dcm_ip_builder_sdk.Configuration(
                    host=self.config.IP_BUILDER_HOST
                )
            )
        )

    def configure_bp(self, bp: Blueprint, *args, **kwargs) -> None:
        @bp.route("/import/external", methods=["POST"])
        @flask_handler(  # unknown query
            handler=services.no_args_handler,
            json=flask_args,
        )
        @flask_handler(  # process import_/build/validation
            handler=get_external_import_handler(
                list(self.config.SUPPORTED_PLUGINS.keys())
            ),
            json=flask_json,
        )
        def import_external(
            import_: ImportConfigExternal,
            build: Optional[dict] = None,
            validation: Optional[dict] = None,
            callback_url: Optional[str] = None
        ):
            """Handle request for import from external system."""
            # validate plugin arg-signature
            valid, msg = self.config.SUPPORTED_PLUGINS[import_.plugin].validate(
                import_.args
            )
            if not valid:
                return Response(
                    f"Bad plugin args: {msg}",
                    status=422,
                    mimetype="text/plain"
                )

            token = self.orchestrator.submit(
                JobConfig(
                    request_body={
                        "import": import_.json,
                        "build": build,
                        "validation": validation,
                        "callback_url": callback_url
                    },
                    context=self.NAME
                )
            )
            return jsonify(token.json), 201

        self._register_abort_job(bp, "/import")

    def get_job(self, config: JobConfig) -> Job:
        return Job(
            cmd=lambda push, data, children: self.import_external(
                push, data, children,
                import_config=ImportConfigExternal.from_json(
                    config.request_body["import"]
                ),
                build_config=config.request_body.get("build"),
                validation_config=config.request_body.get("validation"),
            ),
            hooks={
                "startup": services.default_startup_hook,
                "success": services.default_success_hook,
                "fail": services.default_fail_hook,
                "abort": services.default_abort_hook,
                "completion": services.termination_callback_hook_factory(
                    config.request_body.get("callback_url", None),
                )
            },
            name="Import Module"
        )

    def import_external(
        self, push, report, children: Children,
        import_config: ImportConfigExternal,
        build_config: Optional[dict],
        validation_config: Optional[dict]
    ):
        """
        Job instructions for the '/import/external' endpoint.

        Orchestration standard-arguments:
        push -- (orchestration-standard) push `report` to host process
        report -- (orchestration-standard) common report-object shared
                  via `push`
        children -- (orchestration-standard) `ChildJob`-registry shared
                    via `push`

        Keyword arguments:
        import_config -- an `ImportConfigExternal`-object
        build_config -- jsonable dictionary to be forwarded to the IP
                        Builder
        validation_config - jsonable dictionary to be forwarded to the IP
                            Builder
        """

        # set progress info
        report.progress.verbose = (
            f"importing IEs with plugin '{import_config.plugin}'"
        )
        push()

        # prepare plugin-call
        plugin = self.config.SUPPORTED_PLUGINS[import_config.plugin](
            self.config.IE_OUTPUT,
            timeout=self.config.SOURCE_SYSTEM_TIMEOUT,
            max_retries=self.config.SOURCE_SYSTEM_TIMEOUT_RETRIES
        )
        log_id = f"0@{plugin.name}-plugin"
        report.children = {}
        report.children[log_id] = BaseReport(
            host=report.host,
            token=report.token,
            args=report.args["import"]["args"]
        )
        plugin.register_progress_target(
            report.children[log_id].progress,
            push
        )
        push()

        # make call to plugin
        result = plugin.get(**plugin.complete(import_config.args))

        # process result
        for ie in result.ies.values():
            ie.log_id = log_id
        report.data.ies = result.ies
        success = all(
            ie.fetched_payload for ie in result.ies.values()
        ) and len(result.log.pick(Context.ERROR)) == 0
        report.log.log(
            Context.INFO,
            body=f"Collected {len(result.ies.values())} IE(s) with "
            + f"""{len(
                [ie for ie in result.ies.values() if not ie.fetched_payload]
            )} error(s)."""
        )
        report.children[log_id].log = result.log
        push()

        # exit if no ie collected
        if len(result.ies) == 0:
            report.data.success = success
            report.log.log(
                Context.INFO,
                body="List of IEs is empty."
            )
            push()
            return

        # exit if no build-info provided
        if build_config is None:
            report.data.success = success
            report.log.log(
                Context.INFO,
                body="Skip building IPs (request does not contain build-"
                + "information)."
            )
            push()
            return

        # Build IPs
        report.data.ips = {}
        for ie_id, ie in report.data.ies.items():
            report.progress.verbose = f"building IP from IE '{ie_id}'"
            push()
            if not ie.fetched_payload:
                report.log.log(
                    Context.INFO,
                    body=f"Skip building IP from IE '{ie_id}' (missing payload)."
                )
                push()
                continue
            # make call to ip-builder
            logid = f"{ie_id}@ip_builder"
            external_report = self.build_ip(
                push=push, report=report, children=children,
                logid=logid,
                ie_path=ie.path,
                build_config=build_config,
                validation_config=validation_config,
            )

            if external_report is None:
                # skip if no report from IP Builder was retrieved
                success = False
                report.log.log(
                    Context.ERROR,
                    body=f"IP Builder did not build IP from IE '{ie_id}' "
                    + "(got no report from service)."
                )
                push()
                continue

            success = success and external_report["data"]["valid"]

            if "path" not in external_report["data"]:
                # skip if no path is included in the IP Builder report
                success = False
                report.log.log(
                    Context.ERROR,
                    body=f"IP Builder did not build IP from IE '{ie_id}' "
                    + "(missing 'path' in response)."
                )
                push()
                continue

            # Fill up the IP object
            ip_id = ie_id.replace("ie", "ip")
            report.data.ips[ip_id] = IP(
                path=external_report["data"]["path"],
                ie_identifier=ie_id,
                valid=external_report["data"]["valid"],
                log_id=logid
            )

            # Add the IP-identifier in the corresponding IE-object
            report.data.ies[ie_id].ip_identifier = ip_id
            push()
        report.log.log(
            Context.INFO,
            body=f"Built {len(report.data.ips.values())} IP(s) with "
            + f"""{len(
                [ip for ip in report.data.ips.values() if not ip.valid]
            )} error(s)."""
        )
        report.data.success = success
        push()

    def build_ip(
        self, push, report: BaseReport, children: Children,
        logid: str,
        ie_path: Path,
        build_config: dict,
        validation_config: Optional[dict],
    ) -> Optional[JSONable]:
        """
        Build an IP from an IE using an IP-Builder service.

        Orchestration standard-arguments:
        push -- (orchestration-standard) push `data` to host process
        children -- (orchestration-standard) `ChildJob`-registry shared
                    via `push`

        Keyword arguments:
        report -- `Report`-object associated with this `Job`
        logid -- id of the external log
        ie_path -- the path to the IE that will be used to build the IP
        build_config -- jsonable dictionary to be forwarded to the IP
                        Builder
        validation_config - jsonable dictionary to be forwarded to the IP
                            Builder
        """

        # Initialize request_body
        request_body = {
            "build": (build_config | {"target": {"path": str(ie_path)}})
        }
        if validation_config is not None:
            request_body["validation"] = validation_config

        try:
            response = self.ip_builder_api.build(request_body)
        except ValidationError as exc_info:
            report.log.log(
                Context.ERROR,
                body="Malformed request, not compatible with IP Builder API: "
                + f"{exc_info.errors()}.",
                origin="Import Module"
            )
            push()
            return None
        except dcm_ip_builder_sdk.rest.ApiException as exc_info:
            report.log.log(
                Context.ERROR,
                body=f"IP Builder rejected submission: {exc_info.body} "
                    + f"({exc_info.status}).",
                origin="IP Builder"
            )
            push()
            return None
        except urllib3.exceptions.MaxRetryError:
            report.log.log(
                Context.ERROR,
                body="IP Builder-service unavailable."
            )
            return None

        children.add(
            ChildJobEx(
                token=response.value,
                url=self.config.IP_BUILDER_HOST,
                abort_path="/build",
                id_=logid
            ),
            f"IP Builder-{response.value}"
        )
        report.log.log(
            Context.INFO,
            body=f"IP Builder accepted submission: token={response.value}."
        )
        push()

        # TODO: implement via callback
        # wait until finished (i.e. `get_report` returns a status != 503)
        _elapsed = 0
        while True:
            sleep(0.25)
            # handle any API-exceptions
            try:
                external_report = \
                    self.ip_builder_api.get_report_with_http_info(
                        token=response.value
                    )
                if external_report.status_code == 200:
                    break
            except dcm_ip_builder_sdk.rest.ApiException as exc_info:
                if exc_info.status == 503:
                    try:
                        report.children[logid] = json.loads(exc_info.data)
                    except json.JSONDecodeError:
                        pass
                    else:
                        push()
                else:
                    report.log.log(
                        Context.ERROR,
                        body=f"IP Builder returned with: {exc_info.body} "
                        + f"({exc_info.status})."
                    )
                    children.remove(f"IP Builder-{response.value}")
                    push()
                    return None
            if _elapsed/4 > self.config.IP_BUILDER_JOB_TIMEOUT:
                report.log.log(
                    Context.ERROR,
                    body=f"IP Builder timed out after {_elapsed/4} seconds."
                )
                children.remove(f"IP Builder-{response.value}")
                push()
                return None
            _elapsed = _elapsed + 1

        children.remove(f"IP Builder-{response.value}")
        report.children[logid] = external_report.data.to_dict()
        push()
        return report.children[logid]
