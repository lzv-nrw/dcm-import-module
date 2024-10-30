"""
Import View-class definition
"""

from typing import Optional
from pathlib import Path

from flask import Blueprint, jsonify
from data_plumber_http.decorators import flask_handler, flask_args, flask_json
from dcm_common import LoggingContext as Context, services
from dcm_common.orchestration import JobConfig, Job, Children
from dcm_common.util import list_directory_content
import dcm_ip_builder_sdk

from dcm_import_module.config import AppConfig
from dcm_import_module.models import ImportConfigInternal, IP, Report
from dcm_import_module.handlers import get_internal_import_handler


class IPBuilderAdapter(services.ServiceAdapter):
    """`ServiceAdapter` for the IP Builder service."""
    _SERVICE_NAME = "IP Builder"
    _SDK = dcm_ip_builder_sdk

    def _get_api_clients(self):
        client = self._SDK.ApiClient(self._SDK.Configuration(host=self._url))
        return self._SDK.DefaultApi(client), self._SDK.ValidationApi(client)

    def _get_api_endpoint(self):
        return self._api_client.validate_ip

    def _build_request_body(self, base_request_body, target):
        if target is not None:
            if "validation" not in base_request_body:
                base_request_body["validation"] = {}
            base_request_body["validation"]["target"] = target
        return base_request_body

    def success(self, info) -> bool:
        return info.report.get("data", {}).get("valid", False)


class InternalImportView(services.OrchestratedView):
    """View-class for ip-import from internal storage."""
    NAME = "internal-import"

    def __init__(
        self, config: AppConfig, *args, **kwargs
    ) -> None:
        super().__init__(config, *args, **kwargs)

        # ip-builder adapter
        self.ip_builder_adapter = IPBuilderAdapter(
            self.config.IP_BUILDER_HOST,
            interval=0.25,
            timeout=self.config.IP_BUILDER_JOB_TIMEOUT
        )

    def configure_bp(self, bp: Blueprint, *args, **kwargs) -> None:
        @bp.route("/import/internal", methods=["POST"])
        @flask_handler(  # unknown query
            handler=services.no_args_handler,
            json=flask_args,
        )
        @flask_handler(  # process import_/validation
            handler=get_internal_import_handler(self.config.FS_MOUNT_POINT),
            json=flask_json,
        )
        def import_internal(
            import_: ImportConfigInternal,
            validation: Optional[dict] = None,
            callback_url: Optional[str] = None
        ):
            """Handle request for import from internal storage."""
            token = self.orchestrator.submit(
                JobConfig(
                    request_body={
                        "import": import_.json,
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
            cmd=lambda push, data, children: self.import_internal(
                push, data, children,
                import_config=ImportConfigInternal.from_json(
                    config.request_body["import"]
                ),
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

    @staticmethod
    def _is_ip(path: Path):
        """Returns whether a directory is a plausible IP target."""
        if not path.is_dir():
            return False
        return (path / "bagit.txt").is_file() and (path / "data").is_dir()

    def import_internal(
        self, push, report: Report, children: Children,
        import_config: ImportConfigInternal,
        validation_config: Optional[dict]
    ):
        """
        Job instructions for the '/import/internal' endpoint.

        Orchestration standard-arguments:
        push -- (orchestration-standard) push `data` to host process
        report -- (orchestration-standard) common data-object shared via
                  `push`
        children -- (orchestration-standard) `ChildJob`-registry shared
                    via `push`

        Keyword arguments:
        import_config -- an `ImportConfigInternal`-object
        validation_config - jsonable dictionary to be forwarded to the IP
                            Builder
        """

        if import_config.batch:
            report.log.log(
                Context.INFO,
                body=f"Collecting IPs from '{import_config.target.path}'."
            )
            progress_verbose_base_msg = (
                f"collecting IPs from '{import_config.target.path}'"
            )
        else:
            report.log.log(
                Context.INFO,
                body=f"Collecting IP at '{import_config.target.path}'."
            )
            progress_verbose_base_msg = (
                f"collecting IP at '{import_config.target.path}'"
            )
        report.progress.verbose = progress_verbose_base_msg
        report.data.ips = {}
        push()

        if import_config.batch:
            potential_ips = list_directory_content(
                import_config.target.path,
                condition_function=lambda p: p.is_dir()
            )
        else:
            potential_ips = [import_config.target.path]

        for i, ip in enumerate(potential_ips):
            report.progress.verbose = (
                f"{progress_verbose_base_msg} ({i + 1}/{len(potential_ips)})"
            )
            push()
            if not self._is_ip(ip):
                report.log.log(
                    Context.WARNING,
                    body=f"Skipping directory '{ip}': Implausible target."
                )
                push()
                continue
            report.data.ips[ip.name] = IP(ip)

        if validation_config is None:
            report.data.success = True
            report.log.log(
                Context.INFO,
                body="Skip validating IPs (request does not contain "
                + "validation-information)."
            )
            push()
            return

        report.children = {}
        errors = 0
        for ip_id, ip in report.data.ips.items():
            report.progress.verbose = (
                f"awaiting validation result for '{ip.path}'"
            )
            ip.log_id = f"{ip_id}@ip_builder"
            report.children[ip.log_id] = {}
            push()
            self.ip_builder_adapter.run(
                base_request_body=validation_config,
                target={"path": str(ip.path)},
                info=(
                    info := services.APIResult(
                        report=report.children[ip.log_id]
                    )
                ),
                post_submission_hooks=(
                    # link to children
                    children.link_ex(
                        url=self.config.IP_BUILDER_HOST,
                        abort_path="/validate",
                        tag=ip_id,
                        child_id=ip.log_id,
                        post_link_hook=push
                    ),
                ),
                update_hooks=(
                    lambda _: push(),
                )
            )
            try:
                children.remove(ip_id)
            except KeyError:
                # submission via adapter gave `None`; nothing to abort
                pass
            ip.valid = self.ip_builder_adapter.success(info)
            if not ip.valid:
                errors += 1
            report.log.log(
                Context.INFO,
                body=f"IP '{ip}' is {'valid' if ip.valid else 'invalid'}."
            )
            push()
        report.data.success = errors == 0
        report.log.log(
            Context.INFO if report.data.success else Context.ERROR,
            body=f"There were {errors} errors during validation."
        )
        push()
