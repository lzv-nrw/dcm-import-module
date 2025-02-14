"""Test module for `ObjectValidationAdapter`."""

import pytest
from dcm_common.services import APIResult

from dcm_import_module.components import ObjectValidationAdapter


@pytest.fixture(name="port")
def _port():
    return 8080


@pytest.fixture(name="url")
def _url(port):
    return f"http://localhost:{port}"


@pytest.fixture(name="adapter")
def _adapter(url):
    return ObjectValidationAdapter(url)


@pytest.fixture(name="target")
def _target():
    return {"path": "ip/59438ebf-75e0-4345-8d6b-132a57e1e4f5"}


@pytest.fixture(name="request_body")
def _request_body():
    return {
        "validation": {
            "plugins": {
                "file integrity": {"plugin": "integrity-bagit", "args": {}}
            }
        }
    }


@pytest.fixture(name="token")
def _token():
    return {
        "value": "eb7948a58594df3400696b6ce12013b0e26348ef27e",
        "expires": True,
        "expires_at": "2024-08-09T13:15:10+00:00",
    }


@pytest.fixture(name="report")
def _report(url, token, request_body):
    return {
        "host": url,
        "token": token,
        "args": request_body,
        "progress": {
            "status": "completed",
            "verbose": "Job terminated normally.",
            "numeric": 100,
        },
        "log": {
            "EVENT": [
                {
                    "datetime": "2024-08-09T12:15:10+00:00",
                    "origin": "Object Validator",
                    "body": "Some event",
                },
            ]
        },
        "data": {"valid": True, "success": True, "details": {}},
    }


@pytest.fixture(name="report_fail")
def _report_fail(report):
    report["data"]["success"] = False
    report["data"]["valid"] = False
    return report


@pytest.fixture(name="service_app")
def _service_app(port, token, report, run_service):
    run_service(
        routes=[
            ("/validate", lambda: (token, 201), ["POST"]),
            ("/report", lambda: (report, 200), ["GET"]),
        ],
        port=port,
    )


@pytest.fixture(name="service_app_fail")
def _service_app_fail(port, token, report_fail, run_service):
    run_service(
        routes=[
            ("/validate", lambda: (token, 201), ["POST"]),
            ("/report", lambda: (report_fail, 200), ["GET"]),
        ],
        port=port,
    )


def fix_report_args(info: APIResult, target) -> None:
    """Fixes args in report (missing due to faked service)"""
    info.report["args"]["validation"]["target"] = target


def test_run(
    adapter: ObjectValidationAdapter,
    request_body,
    target,
    report,
    service_app,
):
    """Test method `run` of `ObjectValidationAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    fix_report_args(info, target)
    assert info.completed
    assert info.success
    assert info.report == report


def test_run_fail(
    adapter: ObjectValidationAdapter,
    request_body,
    target,
    report_fail,
    service_app_fail,
):
    """Test method `run` of `ObjectValidationAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    fix_report_args(info, target)
    assert info.completed
    assert not info.success
    assert info.report == report_fail


def test_success(
    adapter: ObjectValidationAdapter, request_body, target, service_app
):
    """Test property `success` of `ObjectValidationAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    assert adapter.success(info)


def test_success_fail(
    adapter: ObjectValidationAdapter,
    request_body,
    target,
    service_app_fail,
):
    """Test property `success` of `ObjectValidationAdapter`."""
    adapter.run(request_body, target, info := APIResult())
    assert not adapter.success(info)
    assert not adapter.valid(info)
