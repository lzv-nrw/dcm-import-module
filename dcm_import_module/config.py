"""Configuration module for the 'DCM Import Module'-app."""

import os
from pathlib import Path
from importlib.metadata import version

import yaml
from dcm_common.services import FSConfig, OrchestratedAppConfig
import dcm_import_module_api

from dcm_import_module.plugins import OAIPMH, DemoPlugin


class AppConfig(FSConfig, OrchestratedAppConfig):
    """Configuration for the 'Import Module'-app."""

    # ------ IMPORT ------
    SOURCE_SYSTEM_TIMEOUT = \
        float(os.environ.get("SOURCE_SYSTEM_TIMEOUT") or 30)
    SOURCE_SYSTEM_TIMEOUT_RETRIES = \
        int(os.environ.get("SOURCE_SYSTEM_TIMEOUT_RETRIES") or 3)
    SOURCE_SYSTEM_TIMEOUT_RETRY_INTERVAL = \
        int(os.environ.get("SOURCE_SYSTEM_TIMEOUT_RETRY_INTERVAL") or 360)
    # determine if test-plugin should available
    USE_TEST_PLUGIN = (int(os.environ.get("USE_TEST_PLUGIN") or 0)) == 1
    # output directory for ie-extraction (relative to FS_MOUNT_POINT)
    IE_OUTPUT = Path(os.environ.get("IE_OUTPUT") or "ie/")
    # available plugins
    SUPPORTED_PLUGINS = {
        OAIPMH.name: OAIPMH,
    } | (
        {DemoPlugin.name: DemoPlugin}
        if USE_TEST_PLUGIN else {}
    )
    # ------ CALL IP BUILDER ------
    IP_BUILDER_HOST = \
        os.environ.get("IP_BUILDER_HOST") or "http://localhost:8083"
    IP_BUILDER_JOB_TIMEOUT = \
        int(os.environ.get("IP_BUILDER_JOB_TIMEOUT") or "3600")

    # ------ API ------
    API_DOCUMENT = Path(dcm_import_module_api.__file__).parent / "openapi.yaml"
    API = yaml.load(
        API_DOCUMENT.read_text(encoding="utf-8"),
        Loader=yaml.SafeLoader
    )

    def set_identity(self) -> None:
        super().set_identity()
        self.CONTAINER_SELF_DESCRIPTION["description"] = (
            "This API allows the collection of IEs (Intellectual Entities) "
            + "from a source system using protocol-specific plugins (e.g., "
            + "via OAI-PMH) and their conversion into IPs (Information "
            + "Packages) by utilizing an IP Builder-service."
        )

        # version
        self.CONTAINER_SELF_DESCRIPTION["version"]["api"] = (
            self.API["info"]["version"]
        )
        self.CONTAINER_SELF_DESCRIPTION["version"]["app"] = version(
            "dcm-import-module"
        )

        # configuration
        # - settings
        settings = self.CONTAINER_SELF_DESCRIPTION["configuration"]["settings"]
        settings["import"] = {
            "output": str(self.IE_OUTPUT),
            "timeout": {
                "duration": int(self.SOURCE_SYSTEM_TIMEOUT),
                "max_retries": self.SOURCE_SYSTEM_TIMEOUT_RETRIES,
                "retry_interval": self.SOURCE_SYSTEM_TIMEOUT_RETRY_INTERVAL
            }
        }
        settings["build"] = {
            "timeout": {
                "duration": self.IP_BUILDER_JOB_TIMEOUT
            }
        }
        # - plugins
        self.CONTAINER_SELF_DESCRIPTION["configuration"]["plugins"] = {
            plugin.name: {
                "name": plugin.name,
                "description": plugin.description,
                "signature": plugin.signature,
                "dependencies": plugin.dependencies_version,
            } for plugin in self.SUPPORTED_PLUGINS.values()
        }
        # - services
        self.CONTAINER_SELF_DESCRIPTION["configuration"]["services"] = {
            "IP Builder": self.IP_BUILDER_HOST
        }
