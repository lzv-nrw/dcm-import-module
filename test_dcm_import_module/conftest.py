from pathlib import Path

import pytest
from flask import jsonify, request
from dcm_common.services.tests import (
    fs_setup, fs_cleanup, external_service, run_service, wait_for_report
)

from dcm_import_module import config
from dcm_import_module.plugins import DemoPlugin


@pytest.fixture(scope="session", name="file_storage")
def _file_storage():
    return Path("test_dcm_import_module/file_storage/")


@pytest.fixture(scope="session", name="fixtures")
def _fixtures():
    path = Path("test_dcm_import_module/fixtures/")
    path.mkdir(parents=True, exist_ok=True)
    return path


@pytest.fixture(scope="session", autouse=True)
def disable_extension_logging():
    """
    Disables the stderr-logging via the helper method `print_status`
    of the `dcm_common.services.extensions`-subpackage.
    """
    # pylint: disable=import-outside-toplevel
    from dcm_common.services.extensions.common import PrintStatusSettings

    PrintStatusSettings.silent = True


@pytest.fixture(name="testing_config")
def _testing_config(file_storage):
    """Returns test-config"""
    # setup config-class
    class TestingConfig(config.AppConfig):
        TESTING = True
        FS_MOUNT_POINT = file_storage
        SUPPORTED_PLUGINS = [DemoPlugin]
        ORCHESTRA_DAEMON_INTERVAL = 0.01
        ORCHESTRA_WORKER_INTERVAL = 0.01
        ORCHESTRA_WORKER_ARGS = {"messages_interval": 0.01}
        SERVICE_POLL_INTERVAL = 0.1

    return TestingConfig


@pytest.fixture(name="minimal_request_body_external")
def _minimal_request_body_external():
    return {
        "import": {
            "plugin": "demo",
            "args": {
                "number": 1,
                "randomize": False
            },
        },
        "build": {
            "mappingPlugin": {"plugin": "oai-mapper", "args": {}},
        },
        "objectValidation": {
            "plugins": {},
        }
    }


@pytest.fixture(name="minimal_request_body_internal")
def _minimal_request_body_internal():
    return {
        "import": {
            "target": {"path": "path/to/ips"},
        },
        "specificationValidation": {
            "BagItProfile": "bagit_profiles/dcm_bagit_profile_v1.0.0.json",
        },
        "objectValidation": {
            "plugins": {},
        }
    }


@pytest.fixture(name="fake_build_report")
def _fake_build_report():
    return {
        "host": "http://localhost:8081",
        "token": {
            "value": "abcdef",
            "expires": True,
            "expires_at": "2024-01-01T00:00:01+01:00"
        },
        "args": {},
        "progress": {"status": "completed", "verbose": "Done", "numeric": 100},
        "log": {},
        "data": {
            "requestType": "build",
            "path": "ip/abcde-12345-fghijk-67890",
            "valid": True,
            "success": True,
            "details": {},
        }
    }


@pytest.fixture(name="fake_validation_report")
def _fake_validation_report():
    return {
        "host": "http://localhost:8082",
        "token": {
            "value": "abcdef",
            "expires": True,
            "expires_at": "2024-01-01T00:00:01+01:00"
        },
        "args": {},
        "progress": {"status": "completed", "verbose": "Done", "numeric": 100},
        "log": {},
        "data": {
            "requestType": "validation",
            "valid": True,
            "success": True,
            "details": {
                "bagit_profile": {
                    "valid": True,
                    "success": True,
                    "log": {}
                }
            }
        }
    }


@pytest.fixture(name="fake_build_report_fail")
def _fake_build_report_fail(fake_build_report):
    fake_build_report["data"]["valid"] = False
    return fake_build_report


@pytest.fixture(name="fake_validation_report_fail")
def _fake_validation_report_fail(fake_validation_report):
    fake_validation_report["data"]["valid"] = False
    fake_validation_report["data"]["details"]["bagit_profile"]["valid"] = False
    return fake_validation_report


@pytest.fixture(name="fake_builder_service")
def _fake_builder_service(
    fake_build_report, fake_validation_report, external_service
):
    return external_service(
        routes=[
            (
                "/build",
                lambda: (jsonify(value="abcdef", expires=False), 201),
                ["POST"],
            ),
            (
                "/validate",
                lambda: (jsonify(value="ghijkl", expires=False), 201),
                ["POST"],
            ),
            (
                "/report",
                lambda: (
                    (jsonify(**fake_build_report), 200)
                    if request.args["token"] == "abcdef"
                    else (jsonify(**fake_validation_report), 200)
                ),
                ["GET"],
            ),
        ]
    )


@pytest.fixture(name="fake_builder_service_fail")
def _fake_builder_service_fail(
    fake_build_report_fail, fake_validation_report_fail, external_service
):
    return external_service(
        routes=[
            (
                "/build",
                lambda: (jsonify(value="abcdef", expires=False), 201),
                ["POST"],
            ),
            (
                "/validate",
                lambda: (jsonify(value="ghijkl", expires=False), 201),
                ["POST"],
            ),
            (
                "/report",
                lambda: (
                    (jsonify(**fake_build_report_fail), 200)
                    if request.args["token"] == "abcdef"
                    else (jsonify(**fake_validation_report_fail), 200)
                ),
                ["GET"],
            ),
        ]
    )
