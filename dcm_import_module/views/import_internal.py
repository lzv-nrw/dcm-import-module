"""
Import View-class definition
"""

from typing import Optional
from pathlib import Path
from random import sample

from flask import Blueprint, jsonify
from data_plumber_http.decorators import flask_handler, flask_args, flask_json
from dcm_common import LoggingContext as Context, Logger, services
from dcm_common.orchestration import JobConfig, Job, Children
from dcm_common.util import list_directory_content

from dcm_import_module.config import AppConfig
from dcm_import_module.models import ImportConfigInternal, IP, Report
from dcm_import_module.handlers import get_internal_import_handler
from dcm_import_module.components import (
    SpecificationValidationAdapter,
    ObjectValidationAdapter,
)


class InternalImportView(services.OrchestratedView):
    """View-class for ip-import from internal storage."""

    NAME = "internal-import"

    def __init__(self, config: AppConfig, *args, **kwargs) -> None:
        super().__init__(config, *args, **kwargs)

        # initialize adapters
        self.spec_validation_adapter = SpecificationValidationAdapter(
            self.config.IP_BUILDER_HOST,
            interval=0.25,
            timeout=self.config.IP_BUILDER_JOB_TIMEOUT,
        )
        self.obj_validation_adapter = ObjectValidationAdapter(
            self.config.OBJECT_VALIDATOR_HOST,
            interval=0.25,
            timeout=self.config.OBJECT_VALIDATOR_JOB_TIMEOUT,
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
            spec_validation: Optional[dict] = None,
            obj_validation: Optional[dict] = None,
            callback_url: Optional[str] = None,
        ):
            """Handle request for import from internal storage."""
            token = self.orchestrator.submit(
                JobConfig(
                    request_body={
                        "import": import_.json,
                        "spec_validation": spec_validation,
                        "obj_validation": obj_validation,
                        "callback_url": callback_url,
                    },
                    context=self.NAME,
                )
            )
            return jsonify(token.json), 201

        self._register_abort_job(bp, "/import")

    def get_job(self, config: JobConfig) -> Job:
        return Job(
            cmd=lambda push, data, children: self.import_internal(
                push,
                data,
                children,
                import_config=ImportConfigInternal.from_json(
                    config.request_body["import"]
                ),
                spec_validation=config.request_body.get("spec_validation"),
                obj_validation=config.request_body.get("obj_validation"),
            ),
            hooks={
                "startup": services.default_startup_hook,
                "success": services.default_success_hook,
                "fail": services.default_fail_hook,
                "abort": services.default_abort_hook,
                "completion": services.termination_callback_hook_factory(
                    config.request_body.get("callback_url", None),
                ),
            },
            name="Import Module",
        )

    @staticmethod
    def _is_ip(path: Path):
        """Returns whether a directory is a plausible IP target."""
        if not path.is_dir():
            return False
        return (path / "bagit.txt").is_file() and (path / "data").is_dir()

    def import_internal(
        self,
        push,
        report: Report,
        children: Children,
        import_config: ImportConfigInternal,
        spec_validation: Optional[dict],
        obj_validation: Optional[dict],
    ) -> None:
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
        spec_validation - jsonable dictionary to be forwarded to the IP
                          Builder
        obj_validation - jsonable dictionary to be forwarded to the
                         Object Validator
        """

        if import_config.batch:
            report.log.log(
                Context.INFO,
                body=f"Collecting IPs from '{import_config.target.path}'.",
            )
            progress_verbose_base_msg = (
                f"collecting IPs from '{import_config.target.path}'"
            )
        else:
            report.log.log(
                Context.INFO,
                body=f"Collecting IP at '{import_config.target.path}'.",
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
                condition_function=lambda p: p.is_dir(),
            )
        else:
            potential_ips = [import_config.target.path]

        # filter implausible
        ips = []
        for i, ip in enumerate(potential_ips):
            report.progress.verbose = (
                f"{progress_verbose_base_msg} ({i + 1}/{len(potential_ips)})"
            )
            push()
            if not self._is_ip(ip):
                report.log.log(
                    Context.WARNING,
                    body=f"Skipping directory '{ip}': Implausible target.",
                )
                push()
                continue
            ips.append(ip)

        # filter in case of test-import
        if import_config.test and self.config.IMPORT_TEST_VOLUME < len(ips):
            ips.sort(key=lambda ip: ip.name)
            report.log.log(
                Context.INFO,
                body=(
                    f"Limiting number of records from {len(ips)} down to "
                    + f"{self.config.IMPORT_TEST_VOLUME} via "
                    + f"'{self.config.IMPORT_TEST_STRATEGY}'-strategy."
                ),
            )
            push()
            match self.config.IMPORT_TEST_STRATEGY:
                case "first":
                    ips = ips[
                        : self.config.IMPORT_TEST_VOLUME
                    ]
                case "random":
                    ips = sample(
                        ips, k=self.config.IMPORT_TEST_VOLUME
                    )
                case _:
                    raise ValueError(
                        "Unknown test-strategy "
                        + f"'{self.config.IMPORT_TEST_STRATEGY}'."
                    )

        for i, ip in enumerate(ips):
            report.data.ips[ip.name] = IP(ip)

        # perform validation if requested
        if spec_validation is None and obj_validation is None:
            report.data.success = True
            report.log.log(
                Context.INFO,
                body="Skip validating IPs (request does not contain "
                + "validation-information).",
            )
            push()
            return

        self._validate(push, report, children, spec_validation, obj_validation)

    def _validate(
        self,
        push,
        report: Report,
        children: Children,
        spec_validation: Optional[dict],
        obj_validation: Optional[dict],
    ) -> None:
        """
        Helper function for running validation during internal import.

        Orchestration standard-arguments:
        push -- (orchestration-standard) push `data` to host process
        report -- (orchestration-standard) common data-object shared via
                  `push`
        children -- (orchestration-standard) `ChildJob`-registry shared
                    via `push`

        Keyword arguments:
        spec_validation - jsonable dictionary to be forwarded to the IP
                          Builder
        obj_validation - jsonable dictionary to be forwarded to the
                         Object Validator
        """

        if report.children is None:
            report.children = {}

        errors = 0
        for ip_id, ip in report.data.ips.items():
            validation_results = []
            for name, adapter, inner_request_body in [
                ("ip_builder", self.spec_validation_adapter, spec_validation),
                (
                    "object_validator",
                    self.obj_validation_adapter,
                    obj_validation,
                ),
            ]:
                report.progress.verbose = (
                    f"requesting validation for '{ip.path}'"
                )
                push()
                if not inner_request_body:
                    continue
                log_id = f"{ip_id}@{name}"
                if not ip.log_id:
                    ip.log_id = []
                ip.log_id.append(log_id)
                report.children[log_id] = {}
                push()
                adapter.run(
                    base_request_body={"validation": inner_request_body},
                    target={"path": str(ip.path)},
                    info=(
                        info := services.APIResult(
                            report=report.children[log_id]
                        )
                    ),
                    post_submission_hooks=(
                        # link to children
                        children.link_ex(
                            url=adapter.url,
                            abort_path="/validate",
                            tag=ip_id,
                            child_id=log_id,
                            post_link_hook=push,
                        ),
                    ),
                    update_hooks=(lambda _: push(),),
                )
                try:
                    children.remove(ip_id)
                except KeyError:
                    # submission via adapter gave `None`; nothing to abort
                    pass
                validation_results.append(adapter.valid(info))
                report.log.merge(
                    Logger(json=info.report.get("log", {})).pick(Context.ERROR)
                )
            ip.valid = all(validation_results)
            if not ip.valid:
                errors += 1
            report.log.log(
                Context.INFO,
                body=(
                    f"IP '{ip.path}' is {'valid' if ip.valid else 'invalid'}."
                ),
            )
            push()
        report.data.success = errors == 0
        report.log.log(
            Context.INFO if report.data.success else Context.ERROR,
            body=f"There were {errors} errors during validation.",
        )
        push()
