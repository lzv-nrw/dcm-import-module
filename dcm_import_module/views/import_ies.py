"""
Import View-class definition
"""

from typing import Optional
import os
from uuid import uuid4

from flask import Blueprint, jsonify, Response, request
from data_plumber_http.decorators import flask_handler, flask_args, flask_json
from dcm_common import LoggingContext, Logger
from dcm_common.orchestra.models import (
    JobConfig,
    JobContext,
    JobInfo,
    ChildJob,
)
from dcm_common import services

from dcm_import_module.models import Report, ImportConfigIEs, IE, IP
from dcm_import_module.handlers import get_ies_import_handler
from dcm_import_module.components import BuildAdapter, ObjectValidationAdapter


class ImportIEsView(services.OrchestratedView):
    """View-class for ie-import."""

    NAME = "ie-import"

    def register_job_types(self):
        self.config.worker_pool.register_job_type(
            self.NAME, self.import_ies, Report
        )

    def configure_bp(self, bp: Blueprint, *args, **kwargs) -> None:
        @bp.route("/import/ies", methods=["POST"])
        @flask_handler(  # unknown query
            handler=services.no_args_handler,
            json=flask_args,
        )
        @flask_handler(  # process import_/build/validation
            handler=get_ies_import_handler(self.config.supported_plugins),
            json=flask_json,
        )
        def import_ies(
            import_: ImportConfigIEs,
            build: Optional[dict] = None,
            obj_validation: Optional[dict] = None,
            token: Optional[str] = None,
            callback_url: Optional[str] = None,
        ):
            """Handle request for importing IEs."""
            try:
                token = self.config.controller.queue_push(
                    token or str(uuid4()),
                    JobInfo(
                        JobConfig(
                            self.NAME,
                            original_body=request.json,
                            request_body={
                                "import": import_.json,
                                "build": build,
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

    def import_ies(self, context: JobContext, info: JobInfo):
        """Job instructions for the '/import/ies' endpoint."""
        os.chdir(self.config.FS_MOUNT_POINT)
        import_config = ImportConfigIEs.from_json(
            info.config.request_body["import"]
        )
        info.report.log.set_default_origin("Import Module")

        # set progress info
        info.report.progress.verbose = (
            f"importing IEs with plugin '{import_config.plugin}'"
        )
        context.push()

        # prepare plugin-call by linking data
        plugin = self.config.supported_plugins[import_config.plugin]
        plugin_context = plugin.create_context(
            info.report.progress.create_verbose_update_callback(
                plugin.display_name
            ),
            context.push,
        )
        info.report.data.ies = plugin_context.result.ies
        plugin_context.result.log = info.report.log
        context.push()

        # make call to plugin
        plugin.get(plugin_context, **import_config.args)

        # process result
        plugin_success = plugin_context.result.success is True
        info.report.log.log(
            LoggingContext.INFO,
            body=f"Collected {len(info.report.data.ies.values())} IE(s) with "
            + f"""{len(
                [
                    ie for ie in info.report.data.ies.values()
                    if not ie.fetched_payload
                ]
            )} error(s).""",
        )
        context.push()

        # exit if no ie collected
        if len(info.report.data.ies) == 0:
            info.report.data.success = plugin_success
            info.report.log.log(
                LoggingContext.INFO, body="List of IEs is empty."
            )
            context.push()

            # make callback; rely on _run_callback to push progress-update
            info.report.progress.complete()
            self._run_callback(
                context, info, info.config.request_body.get("callback_url")
            )
            return

        # exit if no build-info provided
        if info.config.request_body.get("build") is None:
            info.report.data.success = plugin_success
            info.report.log.log(
                LoggingContext.INFO,
                body="Skip building IPs (request does not contain build-"
                + "information).",
            )
            context.push()

            # make callback; rely on _run_callback to push progress-update
            info.report.progress.complete()
            self._run_callback(
                context, info, info.config.request_body.get("callback_url")
            )
            return

        # Build & validate IPs
        # initialize adapters
        build_adapter = BuildAdapter(
            self.config.IP_BUILDER_HOST,
            interval=self.config.SERVICE_POLL_INTERVAL,
            timeout=self.config.SERVICE_TIMEOUT,
        )
        obj_validation_adapter = ObjectValidationAdapter(
            self.config.OBJECT_VALIDATOR_HOST,
            interval=self.config.SERVICE_POLL_INTERVAL,
            timeout=self.config.SERVICE_TIMEOUT,
        )
        info.report.children = {}
        info.report.data.ips = {}
        for ie_id, ie in info.report.data.ies.items():
            # initialize IP-object and link data
            ip_id = ie_id.replace("ie", "ip")
            info.report.data.ies[ie_id].ip_identifier = ip_id
            info.report.data.ips[ip_id] = IP(ie_identifier=ie_id)
            info.report.progress.verbose = f"building IP '{ip_id}'"
            context.push()

            # build & eval
            build_success, build_valid = self._build(
                context,
                info,
                build_adapter,
                ie_id,
                ie,
                ip_id,
                info.report.data.ips[ip_id],
                info.config.request_body["build"],
            )

            # validate & eval
            if (
                not build_success
                or info.config.request_body.get("obj_validation") is None
            ):
                info.report.log.log(
                    LoggingContext.INFO,
                    body=f"Skip object validation for IP '{ip_id}'.",
                )
                context.push()
                payload_success = True
                payload_valid = True
            else:
                payload_success, payload_valid = self._validate_payload(
                    context,
                    info,
                    obj_validation_adapter,
                    ip_id,
                    info.report.data.ips[ip_id],
                    info.config.request_body["obj_validation"],
                )

            if build_success and payload_success:
                info.report.data.ips[ip_id].valid = (
                    build_valid and payload_valid
                )
                info.report.log.log(
                    LoggingContext.INFO,
                    body=(
                        f"IP '{ip_id}' is "
                        + (
                            "valid"
                            if info.report.data.ips[ip_id].valid
                            else "invalid"
                        )
                    ),
                )
            context.push()

        info.report.log.log(
            LoggingContext.INFO,
            body=f"Built {len(info.report.data.ips.values())} IP(s) with "
            + f"""{len(
                [ip for ip in info.report.data.ips.values() if not ip.valid]
            )} error(s).""",
        )
        info.report.data.success = (
            plugin_success
            and build_success
            and payload_success
            and build_valid
            and payload_valid
        )
        context.push()

        # make callback; rely on _run_callback to push progress-update
        info.report.progress.complete()
        self._run_callback(
            context, info, info.config.request_body.get("callback_url")
        )

    def _build(
        self,
        context: JobContext,
        info: JobInfo,
        adapter: BuildAdapter,
        ie_id: str,
        ie: IE,
        ip_id: str,
        ip: IP,
        build: dict,
    ) -> tuple[bool, Optional[bool]]:
        """
        Helper function for building an IP during ie-import.
        Returns tuple of booleans for success and (if successful)
        validation-result.

        Keyword arguments:
        adapter -- adapter for an ip-builder service
        ie_id -- IE identifier
        ie -- IE object
        ip_id -- IP identifier
        ip -- IP object
        build -- jsonable dictionary to be forwarded to the IP Builder
        """

        if not ie.fetched_payload:
            info.report.log.log(
                LoggingContext.INFO,
                body=(
                    f"Skip building IP from IE '{ie_id}' (missing payload)."
                ),
            )
            context.push()
            return False, None

        info.report.log.log(
            LoggingContext.INFO,
            body=f"Building IP '{ip_id}' from IE '{ie_id}'.",
        )
        context.push()

        # make call to ip-builder
        log_id = f"{ip_id}@ip_builder"
        ip.log_id = [log_id]
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
            base_request_body={"token": child_token, "build": build},
            target={"path": str(ie.path)},
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
                        body=f"Got token '{token}' from IP Builder-service.",
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

        info.report.log.merge(
            Logger(json=child_info.report.get("log", {})).pick(
                LoggingContext.ERROR
            )
        )
        ip.path = child_info.report.get("data", {}).get("path")
        context.push()
        if not ip.path:
            info.report.log.log(
                LoggingContext.ERROR,
                body=f"Failed to build IP for IE '{ie_id}' "
                + "(missing 'path' in response).",
            )
            context.push()
            return False, None
        if not child_info.success:
            info.report.log.log(
                LoggingContext.ERROR,
                body=f"Failed to build IP for IE '{ie_id}'.",
            )
            context.push()
            return False, None

        return True, adapter.valid(child_info)

    def _validate_payload(
        self,
        context: JobContext,
        info: JobInfo,
        adapter: ObjectValidationAdapter,
        ip_id: str,
        ip: IP,
        obj_validation: dict,
    ) -> tuple[bool, Optional[bool]]:
        """
        Helper function for running validation during ie-import.
        Returns tuple of booleans for success and (if successful)
        validation-result.

        Keyword arguments:
        adapter -- adapter for an object-validator service
        ip_id - IP identifier
        ip - IP object
        obj_validation - jsonable dictionary to be forwarded to the
                         Object Validator
        """
        info.report.log.log(
            LoggingContext.INFO, body=f"Validating payload of IP '{ip_id}'."
        )
        info.report.progress.verbose = f"validating IP '{ip_id}'"
        log_id = f"{ip_id}@object_validator"
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
                "validation": obj_validation,
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
                            f"Got token '{token}' from Object Validator-"
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

        info.report.log.merge(
            Logger(json=child_info.report.get("log", {})).pick(
                LoggingContext.ERROR
            )
        )
        if not child_info.success:
            info.report.log.log(
                LoggingContext.ERROR,
                body=f"Failed payload validation for IP '{ip_id}'.",
            )
        context.push()

        return child_info.success, adapter.valid(child_info)
