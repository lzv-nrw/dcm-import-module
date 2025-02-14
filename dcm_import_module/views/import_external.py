"""
Import View-class definition
"""

from typing import Optional

from flask import Blueprint, jsonify
from data_plumber_http.decorators import flask_handler, flask_args, flask_json
from dcm_common import LoggingContext as Context, Logger
from dcm_common.orchestration import JobConfig, Job, Children
from dcm_common import services

from dcm_import_module.config import AppConfig
from dcm_import_module.models import ImportConfigExternal, IE, IP
from dcm_import_module.handlers import get_external_import_handler
from dcm_import_module.components import BuildAdapter, ObjectValidationAdapter


class ExternalImportView(services.OrchestratedView):
    """View-class for ip-import from external system."""
    NAME = "external-import"

    def __init__(
        self, config: AppConfig, *args, **kwargs
    ) -> None:
        super().__init__(config, *args, **kwargs)

        # initialize adapters
        self.build_adapter = BuildAdapter(
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
        @bp.route("/import/external", methods=["POST"])
        @flask_handler(  # unknown query
            handler=services.no_args_handler,
            json=flask_args,
        )
        @flask_handler(  # process import_/build/validation
            handler=get_external_import_handler(
                self.config.supported_plugins
            ),
            json=flask_json,
        )
        def import_external(
            import_: ImportConfigExternal,
            build: Optional[dict] = None,
            obj_validation: Optional[dict] = None,
            callback_url: Optional[str] = None
        ):
            """Handle request for import from external system."""
            token = self.orchestrator.submit(
                JobConfig(
                    request_body={
                        "import": import_.json,
                        "build": build,
                        "obj_validation": obj_validation,
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
                build=config.request_body.get("build"),
                obj_validation=config.request_body.get("obj_validation"),
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
        build: Optional[dict],
        obj_validation: Optional[dict]
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
        build -- jsonable dictionary to be forwarded to the IP Builder
        obj_validation - jsonable dictionary to be forwarded to the
                         Object Validator
        """

        # set progress info
        report.progress.verbose = (
            f"importing IEs with plugin '{import_config.plugin}'"
        )
        push()

        # prepare plugin-call by linking data
        plugin = self.config.supported_plugins[import_config.plugin]
        context = plugin.create_context(
            report.progress.create_verbose_update_callback(
                plugin.display_name
            ),
            push,
        )
        report.data.ies = context.result.ies
        context.result.log = report.log
        push()

        # make call to plugin
        plugin.get(context, **import_config.args)

        # process result
        plugin_success = context.result.success is True
        report.log.log(
            Context.INFO,
            body=f"Collected {len(report.data.ies.values())} IE(s) with "
            + f"""{len(
                [
                    ie for ie in report.data.ies.values()
                    if not ie.fetched_payload
                ]
            )} error(s).""",
        )
        push()

        # exit if no ie collected
        if len(report.data.ies) == 0:
            report.data.success = plugin_success
            report.log.log(
                Context.INFO,
                body="List of IEs is empty."
            )
            push()
            return

        # exit if no build-info provided
        if build is None:
            report.data.success = plugin_success
            report.log.log(
                Context.INFO,
                body="Skip building IPs (request does not contain build-"
                + "information)."
            )
            push()
            return

        # Build & validate IPs
        report.children = {}
        report.data.ips = {}
        for ie_id, ie in report.data.ies.items():
            # initialize IP-object and link data
            ip_id = ie_id.replace("ie", "ip")
            report.data.ies[ie_id].ip_identifier = ip_id
            report.data.ips[ip_id] = IP(ie_identifier=ie_id)
            report.progress.verbose = f"building IP '{ip_id}'"
            push()

            # build & eval
            build_success, build_valid = self._build(
                push,
                report,
                children,
                ie_id,
                ie,
                ip_id,
                report.data.ips[ip_id],
                build,
            )

            # validate & eval
            if not build_success or obj_validation is None:
                report.log.log(
                    Context.INFO,
                    body=f"Skip object validation for IP '{ip_id}'."
                )
                push()
                payload_success = True
                payload_valid = True
            else:
                payload_success, payload_valid = self._validate_payload(
                    push,
                    report,
                    children,
                    ip_id,
                    report.data.ips[ip_id],
                    obj_validation,
                )

            if build_success and payload_success:
                report.data.ips[ip_id].valid = build_valid and payload_valid
                report.log.log(
                    Context.INFO,
                    body=(
                        f"IP '{ip_id}' is "
                        + (
                            "valid"
                            if report.data.ips[ip_id].valid
                            else "invalid"
                        )
                    ),
                )
            push()

        report.log.log(
            Context.INFO,
            body=f"Built {len(report.data.ips.values())} IP(s) with "
            + f"""{len(
                [ip for ip in report.data.ips.values() if not ip.valid]
            )} error(s)."""
        )
        report.data.success = (
            plugin_success
            and build_success
            and payload_success
            and build_valid
            and payload_valid
        )
        push()

    def _build(
        self,
        push,
        report,
        children: Children,
        ie_id: str,
        ie: IE,
        ip_id: str,
        ip: IP,
        build: dict,
    ) -> tuple[bool, Optional[bool]]:
        """
        Helper function for building an IP during external import.
        Returns tuple of booleans for success and (if successful)
        validation-result.

        Orchestration standard-arguments:
        push -- (orchestration-standard) push `data` to host process
        report -- (orchestration-standard) common data-object shared via
                  `push`
        children -- (orchestration-standard) `ChildJob`-registry shared
                    via `push`

        Keyword arguments:
        ie_id - IE identifier
        ie - IE object
        ip_id - IP identifier
        ip - IP object
        build - jsonable dictionary to be forwarded to the IP Builder
        """

        if not ie.fetched_payload:
            report.log.log(
                Context.INFO,
                body=(
                    f"Skip building IP from IE '{ie_id}' (missing payload)."
                ),
            )
            push()
            return False, None

        report.log.log(
            Context.INFO,
            body=f"Building IP '{ip_id}' from IE '{ie_id}'."
        )
        push()

        # make call to ip-builder
        log_id = f"{ip_id}@ip_builder"
        ip.log_id = [log_id]
        report.children[log_id] = {}
        push()
        self.build_adapter.run(
            base_request_body={"build": build},
            target={"path": str(ie.path)},
            info=(
                info := services.APIResult(
                    report=report.children[log_id]
                )
            ),
            post_submission_hooks=(
                # link to children
                children.link_ex(
                    url=self.build_adapter.url,
                    abort_path="/build",
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

        report.log.merge(
            Logger(json=info.report.get("log", {})).pick(Context.ERROR)
        )
        ip.path = info.report.get("data", {}).get("path")
        push()
        if not ip.path:
            report.log.log(
                Context.ERROR,
                body=f"Failed to build IP for IE '{ie_id}' "
                + "(missing 'path' in response)."
            )
            push()
            return False, None
        if not info.success:
            report.log.log(
                Context.ERROR,
                body=f"Failed to build IP for IE '{ie_id}'."
            )
            push()
            return False, None

        return True, self.build_adapter.valid(info)

    def _validate_payload(
        self,
        push,
        report,
        children: Children,
        ip_id: str,
        ip: IP,
        obj_validation: dict,
    ) -> tuple[bool, Optional[bool]]:
        """
        Helper function for running validation during external import.
        Returns tuple of booleans for success and (if successful)
        validation-result.

        Orchestration standard-arguments:
        push -- (orchestration-standard) push `data` to host process
        report -- (orchestration-standard) common data-object shared via
                  `push`
        children -- (orchestration-standard) `ChildJob`-registry shared
                    via `push`

        Keyword arguments:
        ip_id - IP identifier
        ip - IP object
        obj_validation - jsonable dictionary to be forwarded to the
                         Object Validator
        """
        report.log.log(
            Context.INFO,
            body=f"Validating payload of IP '{ip_id}'."
        )
        report.progress.verbose = f"validating IP '{ip_id}'"
        log_id = f"{ip_id}@object_validator"
        ip.log_id.append(log_id)
        report.children[log_id] = {}
        push()

        self.obj_validation_adapter.run(
            base_request_body={"validation": obj_validation},
            target={"path": str(ip.path)},
            info=(
                info := services.APIResult(
                    report=report.children[log_id]
                )
            ),
            post_submission_hooks=(
                # link to children
                children.link_ex(
                    url=self.obj_validation_adapter.url,
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

        report.log.merge(
            Logger(json=info.report.get("log", {})).pick(Context.ERROR)
        )
        if not info.success:
            report.log.log(
                Context.ERROR,
                body=f"Failed payload validation for IP '{ip_id}'."
            )
        push()

        return info.success, self.obj_validation_adapter.valid(info)
