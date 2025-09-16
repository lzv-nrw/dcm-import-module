"""
Import View-class definition
"""

from typing import Optional
from pathlib import Path
from random import sample
from uuid import uuid4
from shutil import copytree, rmtree

from flask import Blueprint, jsonify, Response, request
from data_plumber_http.decorators import flask_handler, flask_args, flask_json
from dcm_common import LoggingContext, Logger, services
from dcm_common.orchestra.models import (
    JobConfig,
    JobContext,
    JobInfo,
    ChildJob,
)
from dcm_common.util import list_directory_content

from dcm_import_module.models import ImportConfigIPs, IP, Report
from dcm_import_module.handlers import ips_import_handler
from dcm_import_module.components import (
    SpecificationValidationAdapter,
    ObjectValidationAdapter,
)


class ImportIPsView(services.OrchestratedView):
    """View-class for ip-import."""

    NAME = "ip-import"

    def register_job_types(self):
        self.config.worker_pool.register_job_type(
            self.NAME, self.import_ips, Report
        )

    def configure_bp(self, bp: Blueprint, *args, **kwargs) -> None:
        @bp.route("/import/ips", methods=["POST"])
        @flask_handler(  # unknown query
            handler=services.no_args_handler,
            json=flask_args,
        )
        @flask_handler(  # process import_/validation
            handler=ips_import_handler,
            json=flask_json,
        )
        def import_ips(
            import_: ImportConfigIPs,
            spec_validation: Optional[dict] = None,
            obj_validation: Optional[dict] = None,
            token: Optional[str] = None,
            callback_url: Optional[str] = None,
        ):
            """Handle request for importing IPs."""
            # run additional checks not covered by handler
            if import_.target.hotfolder_id is None:
                # no hotfolder
                # * target is not a directory
                if not (
                    self.config.FS_MOUNT_POINT / import_.target.path
                ).is_dir():
                    return Response(
                        f"Directory '{import_.target.path}' is invalid (does "
                        + "not exist or not a directory).",
                        mimetype="text/plain",
                        status=404,
                    )
            else:
                # hotfolder
                # * unknown hotfolder
                if import_.target.hotfolder_id not in self.config.hotfolders:
                    return Response(
                        "Unknown hotfolder identifier "
                        + f"'{import_.target.hotfolder_id}'.",
                        mimetype="text/plain",
                        status=404,
                    )
                # * hotfolder not available
                if not self.config.hotfolders[
                    import_.target.hotfolder_id
                ].mount.is_dir():
                    return Response(
                        "Encountered a bad hotfolder configuration with id "
                        + f"'{import_.target.hotfolder_id}' (hotfolder is not "
                        + "mounted). Please contact the system administrator.",
                        mimetype="text/plain",
                        status=404,
                    )
                # * target is not a directory
                if not (
                    self.config.hotfolders[import_.target.hotfolder_id].mount
                    / import_.target.path
                ).is_dir():
                    return Response(
                        f"Hotfolder directory '{import_.target.path}' is "
                        + "invalid (does not exist or not a directory).",
                        mimetype="text/plain",
                        status=404,
                    )
            try:
                token = self.config.controller.queue_push(
                    token or str(uuid4()),
                    JobInfo(
                        JobConfig(
                            self.NAME,
                            original_body=request.json,
                            request_body={
                                "import": import_.json,
                                "spec_validation": spec_validation,
                                "obj_validation": obj_validation,
                                "callback_url": callback_url,
                            },
                        ),
                        report=Report(
                            host=request.host_url, args=request.json
                        ),
                    ),
                )
            # pylint: disable=broad-exception-caught
            except Exception as exc_info:
                return Response(
                    f"Submission rejected: {exc_info}",
                    mimetype="text/plain",
                    status=500,
                )

            return jsonify(token.json), 201

        self._register_abort_job(bp, "/import")

    @staticmethod
    def _is_ip(path: Path):
        """Returns whether a directory is a plausible IP target."""
        if not path.is_dir():
            return False
        return (path / "bagit.txt").is_file() and (path / "data").is_dir()

    def import_ips(self, context: JobContext, info: JobInfo):
        """Job instructions for the '/import/ips' endpoint."""
        import_config = ImportConfigIPs.from_json(
            info.config.request_body["import"]
        )
        info.report.log.set_default_origin("Import Module")

        if import_config.batch:
            info.report.log.log(
                LoggingContext.INFO,
                body=f"Collecting IPs from '{import_config.target.path}'.",
            )
            progress_verbose_base_msg = (
                f"collecting IPs from '{import_config.target.path}'"
            )
        else:
            info.report.log.log(
                LoggingContext.INFO,
                body=f"Collecting IP at '{import_config.target.path}'.",
            )
            progress_verbose_base_msg = (
                f"collecting IP at '{import_config.target.path}'"
            )
        info.report.progress.verbose = progress_verbose_base_msg
        info.report.data.ips = {}
        context.push()

        if import_config.target.hotfolder_id is None:
            base_directory = self.config.FS_MOUNT_POINT.resolve()
        else:
            if import_config.target.hotfolder_id not in self.config.hotfolders:
                info.report.log.log(
                    LoggingContext.ERROR,
                    body=(
                        "Missing hotfolder configuration with id "
                        + f"'{import_config.target.hotfolder_id}'."
                    ),
                )
                context.push()
                self._run_callback(
                    context, info, info.config.request_body.get("callback_url")
                )
                return
            base_directory = self.config.hotfolders[
                import_config.target.hotfolder_id
            ].mount.resolve()
            if not base_directory.is_dir():
                info.report.log.log(
                    LoggingContext.ERROR,
                    body=(
                        f"Hotfolder '{import_config.target.hotfolder_id}' "
                        + (
                            f"({self.config.hotfolders[import_config.target.hotfolder_id].name}) "
                            if self.config.hotfolders[
                                import_config.target.hotfolder_id
                            ].name
                            is not None
                            else ""
                        )
                        + "is not mounted."
                    ),
                )
                context.push()
                self._run_callback(
                    context, info, info.config.request_body.get("callback_url")
                )
                return
        if import_config.batch:
            potential_ips = list_directory_content(
                base_directory / import_config.target.path,
                condition_function=lambda p: p.is_dir(),
            )
        else:
            potential_ips = [base_directory / import_config.target.path]

        # filter implausible
        ips = []
        for i, ip in enumerate(potential_ips):
            info.report.progress.verbose = (
                f"{progress_verbose_base_msg} ({i + 1}/{len(potential_ips)})"
            )
            context.push()
            if not self._is_ip(ip):
                info.report.log.log(
                    LoggingContext.WARNING,
                    body=f"Skipping directory '{ip}': Implausible target.",
                )
                context.push()
                continue
            ips.append(ip)

        # filter in case of test-import
        if import_config.test and self.config.IMPORT_TEST_VOLUME < len(ips):
            ips.sort(key=lambda ip: ip.name)
            info.report.log.log(
                LoggingContext.INFO,
                body=(
                    f"Limiting number of records from {len(ips)} down to "
                    + f"{self.config.IMPORT_TEST_VOLUME} via "
                    + f"'{self.config.IMPORT_TEST_STRATEGY}'-strategy."
                ),
            )
            context.push()
            match self.config.IMPORT_TEST_STRATEGY:
                case "first":
                    ips = ips[: self.config.IMPORT_TEST_VOLUME]
                case "random":
                    ips = sample(ips, k=self.config.IMPORT_TEST_VOLUME)
                case _:
                    raise ValueError(
                        "Unknown test-strategy "
                        + f"'{self.config.IMPORT_TEST_STRATEGY}'."
                    )

        for i, ip in enumerate(ips):
            # copy to IP_OUTPUT
            dest = (
                self.config.FS_MOUNT_POINT
                / self.config.IP_OUTPUT
                / str(uuid4())
            )
            dest.parent.mkdir(parents=True, exist_ok=True)
            copytree(ip, dest)
            # add to report
            info.report.data.ips[ip.name] = IP(
                dest.relative_to(self.config.FS_MOUNT_POINT)
            )
            context.push()
            # remove source
            rmtree(ip)

        # perform validation if requested
        spec_validation = info.config.request_body.get("spec_validation")
        obj_validation = info.config.request_body.get("obj_validation")
        if spec_validation is None and obj_validation is None:
            info.report.data.success = True
            info.report.log.log(
                LoggingContext.INFO,
                body="Skip validating IPs (request does not contain "
                + "validation-information).",
            )
            context.push()

            # make callback; rely on _run_callback to push progress-update
            info.report.progress.complete()
            self._run_callback(
                context, info, info.config.request_body.get("callback_url")
            )
            return

        self._validate(
            context,
            info,
            (
                None
                if spec_validation is None
                else SpecificationValidationAdapter(
                    self.config.IP_BUILDER_HOST,
                    interval=self.config.SERVICE_POLL_INTERVAL,
                    timeout=self.config.SERVICE_TIMEOUT,
                )
            ),
            (
                None
                if obj_validation is None
                else ObjectValidationAdapter(
                    self.config.OBJECT_VALIDATOR_HOST,
                    interval=self.config.SERVICE_POLL_INTERVAL,
                    timeout=self.config.SERVICE_TIMEOUT,
                )
            ),
            spec_validation,
            obj_validation,
        )

        # make callback; rely on _run_callback to push progress-update
        info.report.progress.complete()
        self._run_callback(
            context, info, info.config.request_body.get("callback_url")
        )

    def _validate(
        self,
        context: JobContext,
        info: JobInfo,
        spec_validation_adapter: Optional[SpecificationValidationAdapter],
        obj_validation_adapter: Optional[ObjectValidationAdapter],
        spec_validation: Optional[dict],
        obj_validation: Optional[dict],
    ) -> None:
        """
        Helper function for running validation during ip-import.

        Keyword arguments:
        spec_validation_adapter -- adapter for an up-builder
        obj_validation_adapter -- adapter for an object-validator
        spec_validation -- jsonable dictionary to be forwarded to the IP
                           Builder
        obj_validation -- jsonable dictionary to be forwarded to the
                          Object Validator
        """

        if info.report.children is None:
            info.report.children = {}

        errors = 0
        for ip_id, ip in info.report.data.ips.items():
            validation_results = []
            for name, adapter, inner_request_body in [
                ("ip_builder", spec_validation_adapter, spec_validation),
                (
                    "object_validator",
                    obj_validation_adapter,
                    obj_validation,
                ),
            ]:
                if not inner_request_body:
                    continue

                info.report.progress.verbose = (
                    f"requesting validation for '{ip.path}'"
                )
                context.push()
                log_id = f"{ip_id}@{name}"
                if not ip.log_id:
                    ip.log_id = []
                ip.log_id.append(log_id)
                info.report.children[log_id] = {}
                child_token = str(uuid4())
                context.add_child(
                    ChildJob(
                        child_token,
                        log_id,
                        adapter.get_abort_callback(
                            child_token, log_id, "Import Module"
                        ),
                    )
                )
                context.push()
                adapter.run(
                    base_request_body={
                        "token": child_token,
                        "validation": inner_request_body,
                    },
                    target={"path": str(ip.path)},
                    info=(
                        child_info := services.APIResult(
                            report=info.report.children[log_id]
                        )
                    ),
                    post_submission_hooks=(
                        # post to log
                        lambda token, info=info: (
                            info.report.log.log(
                                LoggingContext.INFO,
                                body=(
                                    f"Got token '{token}' from IP Builder-"
                                    + "service."
                                ),
                            ),
                            context.push(),
                        ),
                    ),
                    update_hooks=(lambda data: context.push(),),
                )
                try:
                    context.remove_child(child_token)
                except KeyError:
                    # submission via adapter gave `None`; nothing to abort
                    pass
                validation_results.append(adapter.valid(child_info))
                info.report.log.merge(
                    Logger(json=child_info.report.get("log", {})).pick(
                        LoggingContext.ERROR
                    )
                )
            ip.valid = all(validation_results)
            if not ip.valid:
                errors += 1
            info.report.log.log(
                LoggingContext.INFO,
                body=(
                    f"IP '{ip.path}' is {'valid' if ip.valid else 'invalid'}."
                ),
            )
            context.push()
        info.report.data.success = errors == 0
        info.report.log.log(
            (
                LoggingContext.INFO
                if info.report.data.success
                else LoggingContext.ERROR
            ),
            body=f"There were {errors} errors during validation.",
        )
        context.push()
