"""Configuration module for the 'DCM Import Module'-app."""

import os
from pathlib import Path
from importlib.metadata import version

import yaml
from dcm_common.services import FSConfig, OrchestratedAppConfig
import dcm_import_module_api

from dcm_import_module.plugins import OAIPMHPlugin, OAIPMHPlugin2, DemoPlugin


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
    USE_DEMO_PLUGIN = (int(os.environ.get("USE_DEMO_PLUGIN") or 0)) == 1
    # output directory for ie-extraction (relative to FS_MOUNT_POINT)
    IE_OUTPUT = Path(os.environ.get("IE_OUTPUT") or "ie/")
    # test-imports
    IMPORT_TEST_STRATEGY = os.environ.get("IMPORT_TEST_STRATEGY", "first")
    IMPORT_TEST_VOLUME = int(os.environ.get("IMPORT_TEST_VOLUME") or 2)
    # available plugins
    SUPPORTED_PLUGINS = [OAIPMHPlugin, OAIPMHPlugin2] + (
            [DemoPlugin] if USE_DEMO_PLUGIN else []
    )
    # OAI-plugins
    OAI_MAX_RESUMPTION_TOKENS = (
        int(os.environ["OAI_MAX_RESUMPTION_TOKENS"])
        if "OAI_MAX_RESUMPTION_TOKENS" in os.environ
        else None
    )

    # ------ SERVICE ADAPTERS ------
    SERVICE_TIMEOUT = int(os.environ.get("SERVICE_TIMEOUT") or 3600)
    SERVICE_POLL_INTERVAL = int(os.environ.get("SERVICE_POLL_INTERVAL") or 1)
    # ------ CALL IP BUILDER ------
    IP_BUILDER_HOST = \
        os.environ.get("IP_BUILDER_HOST") or "http://localhost:8081"
    # ------ CALL OBJECT VALIDATOR ------
    OBJECT_VALIDATOR_HOST = \
        os.environ.get("OBJECT_VALIDATOR_HOST") or "http://localhost:8082"

    # ------ API ------
    API_DOCUMENT = Path(dcm_import_module_api.__file__).parent / "openapi.yaml"
    API = yaml.load(
        API_DOCUMENT.read_text(encoding="utf-8"),
        Loader=yaml.SafeLoader
    )

    def __init__(self, **kwargs) -> None:
        self.supported_plugins = {}
        for Plugin in self.SUPPORTED_PLUGINS:
            self.supported_plugins[Plugin.name] = Plugin(
                self.IE_OUTPUT,
                timeout=self.SOURCE_SYSTEM_TIMEOUT,
                max_retries=self.SOURCE_SYSTEM_TIMEOUT_RETRIES,
                max_resumption_tokens=self.OAI_MAX_RESUMPTION_TOKENS,
                test_strategy=self.IMPORT_TEST_STRATEGY,
                test_volume=self.IMPORT_TEST_VOLUME,
            )

        super().__init__(**kwargs)

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
            },
            "test": {
                "volume": self.IMPORT_TEST_VOLUME,
                "strategy": self.IMPORT_TEST_STRATEGY,
            }
        }
        settings["build"] = {
            "timeout": {
                "duration": self.SERVICE_TIMEOUT
            }
        }
        # - plugins
        self.CONTAINER_SELF_DESCRIPTION["configuration"]["plugins"] = {
            plugin.name: plugin.json
            for plugin in self.supported_plugins.values()
        }
        # - services
        self.CONTAINER_SELF_DESCRIPTION["configuration"]["services"] = {
            "ip_builder": self.IP_BUILDER_HOST,
            "object_validator": self.OBJECT_VALIDATOR_HOST,
        }
