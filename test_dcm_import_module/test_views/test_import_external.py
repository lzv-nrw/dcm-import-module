"""
Test module for the `dcm_import_module/views/import_external.py`.
"""

from unittest import mock
from pathlib import Path
from time import sleep, time
from uuid import uuid4

import pytest
from flask import jsonify, request as flask_request, Response
from dcm_common import LoggingContext as Context

from dcm_import_module import app_factory
from dcm_import_module.plugins import Interface, OAIPMH
from dcm_import_module.models import PluginResult, IE, Signature


@pytest.fixture(name="minimal_request_body")
def _minimal_request_body():
    return {
        "import": {
            "plugin": "demo",
            "args": {
                "number": 1,
                "randomize": False
            },
        }
    }


def test_import(
    testing_config, minimal_request_body, client, wait_for_report, run_service,
    fake_build_report, fake_builder_service
):
    """Minimal test of /import/external-endpoint with build."""

    run_service(
        app=fake_builder_service,
        port=8083
    )

    # make request for import
    response = client.post(
        "/import/external",
        json=minimal_request_body | {"build": {"configuration": "-"}}
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200
    assert response.status_code == 201
    assert response.mimetype == "application/json"
    assert "value" in response.json

    json = wait_for_report(client, response.json["value"])

    assert json["data"]["success"]
    assert len(json["data"]["IEs"]) == 1
    assert (
        testing_config().FS_MOUNT_POINT / json["data"]["IEs"]["ie0"]["path"]
    ).is_dir()
    assert len(json["data"]["IPs"]) == 1
    assert fake_build_report \
        == json["children"][json["data"]["IPs"]["ip0"]["logId"]]


def test_import_only_ie(
    testing_config, minimal_request_body, client, wait_for_report
):
    """Minimal test of /import/external-endpoint."""

    # make request for import
    response = client.post(
        "/import/external",
        json=minimal_request_body
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    json = wait_for_report(client, response.json["value"])

    assert json["data"]["success"]
    assert (
        testing_config().FS_MOUNT_POINT / json["data"]["IEs"]["ie0"]["path"]
    ).is_dir()
    assert len(json["children"]) == 1
    assert "0@demo-plugin" in json["children"]
    assert json["data"]["IEs"]["ie0"]["IPIdentifier"] is None
    assert any("Skip building" in msg["body"] for msg in json["log"]["INFO"])
    assert "IPs" not in json["data"]


def test_import_empty(
    minimal_request_body, client, wait_for_report,
):
    """Test of /import/external-endpoint if not IEs are generated."""

    # make request for import
    minimal_request_body["import"]["args"]["number"] = 0
    response = client.post(
        "/import/external",
        json=minimal_request_body
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    json = wait_for_report(client, response.json["value"])

    assert json["data"]["success"]
    assert any("List of IEs is empty" in msg["body"] for msg in json["log"]["INFO"])
    assert len(json["data"]["IEs"]) == 0
    assert "IPs" not in json["data"]
    assert len(json["children"]) == 1
    assert "0@demo-plugin" in json["children"]


def test_timeout_of_source_system(
    testing_config, minimal_request_body, wait_for_report, run_service
):
    """Test import behavior when source system times out."""

    class ThisConfig(testing_config):
        SOURCE_SYSTEM_TIMEOUT = 0.1
        SOURCE_SYSTEM_TIMEOUT_RETRIES = 1
        SOURCE_SYSTEM_TIMEOUT_RETRY_INTERVAL = 1
        SUPPORTED_PLUGINS = {
            OAIPMH.name: OAIPMH
        }
    client = app_factory(ThisConfig()).test_client()
    run_service(
        routes=[
            ("/build", lambda: (jsonify(value="abcdef", expires=False), 201), ["POST"]),
            ("/report", lambda: Response("No", 503), ["GET"]),
        ],
        port=8083
    )
    def timeout():
        sleep(2 * ThisConfig().SOURCE_SYSTEM_TIMEOUT)
    run_service(
        routes=[("/get", timeout, ["GET"])],
        port=8082
    )
    # make request for import
    response = client.post(
        "/import/external",
        json={
            "import": {
                "plugin": OAIPMH.name,
                "args": {
                    "transfer_url_info": {
                        "regex": "asd"
                    },
                    "base_url": "http://localhost:8082/get",
                    "metadata_prefix": ""
                }
            },
            "build": {"configuration": "-"}
        }
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200
    json = wait_for_report(client, response.json["value"])

    assert not json["data"]["success"]
    assert Context.ERROR.name in json["children"]["0@oai_pmh-plugin"]["log"]
    assert "timeout" in str(json["children"]["0@oai_pmh-plugin"]["log"])


def test_import_no_path(
    minimal_request_body, client, wait_for_report, run_service,
    fake_build_report
):
    """
    Test of /import/external-endpoint if no path is returned by the IP
    Builder.
    """

    # Remove the path from the fake_build_report
    del fake_build_report["data"]["path"]
    fake_build_report["data"]["valid"] = False
    # Run the IP Builder service
    run_service(
        routes=[
            ("/build", lambda: (jsonify(value="abcdef", expires=False), 201), ["POST"]),
            ("/report", lambda: (jsonify(**fake_build_report), 200), ["GET"]),
        ],
        port=8083
    )

    # make request for import
    response = client.post(
        "/import/external",
        json=minimal_request_body | {"build": {"configuration": "-"}}
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    json = wait_for_report(client, response.json["value"])

    assert not json["data"]["success"]
    assert len(json["data"]["IEs"]) == 1
    assert len(json["data"]["IPs"]) == 0
    assert len(json["children"]) == 2
    assert "ie0@ip_builder" in json["children"]


def test_missing_payload_in_ie(
    client, minimal_request_body, wait_for_report,
    run_service, fake_builder_service
):
    """
    Test of /import/external-endpoint where IE is not complete (by
    faking plugin).
    """

    run_service(
        app=fake_builder_service,
        port=8083
    )

    # fake plugin
    plugin_patch = mock.patch(
        "dcm_import_module.plugins.demo_plugin.DemoPlugin.get",
        return_value=PluginResult(
            ies={
                "ie0": IE(Path("."), fetched_payload=False),
                "ie1": IE(Path("."), fetched_payload=True),
            }
        )
    )
    plugin_patch.start()

    # make request for import
    response = client.post(
        "/import/external",
        json=minimal_request_body | {"build": {"configuration": "-"}}
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    json = wait_for_report(client, response.json["value"])

    assert not json["data"]["success"]
    assert any("Skip building" in msg["body"] for msg in json["log"]["INFO"])
    assert len(json["data"]["IEs"]) == 2
    assert not json["data"]["IEs"]["ie0"]["fetchedPayload"]
    assert json["data"]["IEs"]["ie0"]["IPIdentifier"] is None
    assert json["data"]["IEs"]["ie1"]["fetchedPayload"]
    assert json["data"]["IEs"]["ie1"]["IPIdentifier"] == "ip1"
    assert len(json["data"]["IPs"]) == 1
    assert len(json["children"]) == 2
    assert "0@demo-plugin" in json["children"]
    assert "ie1@ip_builder" in json["children"]
    assert json["data"]["IPs"]["ip1"]["IEIdentifier"] == "ie1"

    plugin_patch.stop()


@pytest.mark.parametrize(
    "valid",
    [True, False],
    ids=["valid", "invalid"]
)
def test_processing_of_invalid_ip(
    client, minimal_request_body, wait_for_report,
    run_service, fake_build_report, valid
):
    """
    Test of /import/external-endpoint where builder returns with invalid
    flag.
    """

    fake_build_report["data"]["valid"] = valid
    run_service(
        routes=[
            ("/build", lambda: (jsonify(value="abcdef", expires=False), 201), ["POST"]),
            ("/report", lambda: (jsonify(**fake_build_report), 200), ["GET"]),
        ],
        port=8083
    )
    # make request for import
    response = client.post(
        "/import/external",
        json=minimal_request_body | {"build": {"configuration": "-"}}
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    json = wait_for_report(client, response.json["value"])

    assert json["data"]["success"] == valid
    assert len(json["data"]["IEs"]) == 1
    assert len(json["data"]["IPs"]) == 1
    assert json["data"]["IPs"]["ip0"]["valid"] == valid


def test_arg_forwarding_to_ip_builder(
    client, minimal_request_body, wait_for_report,
    run_service, fake_build_report
):
    """
    Test whether arguments in "build" and "validation" are forwarded to
    builder service correctly.
    """

    def post():
        fake_build_report["args"] = flask_request.json
        return (jsonify(value="abcdef", expires=False), 201)
    run_service(
        routes=[
            ("/build", post, ["POST"]),
            ("/report", lambda: (jsonify(**fake_build_report), 200), ["GET"]),
        ],
        port=8083
    )
    # make request for import
    extra_args = {
        "build": {"key": "value"},
        "validation": {"another-key": "another-value"}
    }
    response = client.post(
        "/import/external",
        json=minimal_request_body | extra_args
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    json = wait_for_report(client, response.json["value"])

    assert "ie0@ip_builder" not in json["children"]


def test_no_connection_to_ip_builder(
    client, minimal_request_body, wait_for_report
):
    """
    Test behavior of import when no connection to builder can be established.
    """
    # make request for import
    response = client.post(
        "/import/external",
        json=minimal_request_body | {"build": {"configuration": "-"}}
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    json = wait_for_report(client, response.json["value"])

    assert not json["data"]["success"]
    assert len(json["log"]["ERROR"]) == 2
    assert any("unavailable" in msg["body"] for msg in json["log"]["ERROR"])
    assert any("did not build" in msg["body"] for msg in json["log"]["ERROR"])
    assert any("ie0" in msg["body"] for msg in json["log"]["ERROR"])


def test_rejection_by_ip_builder(
    client, minimal_request_body, wait_for_report, run_service
):
    """
    Test behavior of import when builder rejects any request.
    """

    rejection_msg = "No, will not process something like that."
    rejection_status = 422
    run_service(
        routes=[
            ("/build", lambda: Response(rejection_msg, status=rejection_status), ["POST"]),
        ],
        port=8083
    )
    # make request for import
    response = client.post(
        "/import/external",
        json=minimal_request_body | {"build": {"configuration": "-"}}
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    json = wait_for_report(client, response.json["value"])

    assert not json["data"]["success"]
    assert len(json["log"]["ERROR"]) == 2
    assert any(rejection_msg in msg["body"] for msg in json["log"]["ERROR"])
    assert any(str(rejection_status) in msg["body"] for msg in json["log"]["ERROR"])
    assert any("did not build" in msg["body"] for msg in json["log"]["ERROR"])


def test_timeout_of_ip_builder(
    testing_config, minimal_request_body, wait_for_report, run_service
):
    """Test import behavior when builder times out."""

    class ThisConfig(testing_config):
        IP_BUILDER_JOB_TIMEOUT = 0.25
    client = app_factory(ThisConfig()).test_client()
    run_service(
        routes=[
            ("/build", lambda: (jsonify(value="abcdef", expires=False), 201), ["POST"]),
            ("/report", lambda: Response("No", 503), ["GET"]),
        ],
        port=8083
    )
    # make request for import
    response = client.post(
        "/import/external",
        json=minimal_request_body | {"build": {"configuration": "-"}}
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    json = wait_for_report(client, response.json["value"])

    assert not json["data"]["success"]
    assert len(json["log"]["ERROR"]) == 2
    assert any("Builder timed out" in msg["body"] for msg in json["log"]["ERROR"])
    assert any("did not build" in msg["body"] for msg in json["log"]["ERROR"])


def test_unknown_report_from_ip_builder(
    client, minimal_request_body, wait_for_report, run_service
):
    """Test import behavior when builder 'forgets' report."""

    run_service(
        routes=[
            ("/build", lambda: (jsonify(value="abcdef", expires=False), 201), ["POST"]),
            ("/report", lambda: Response("What?", 404), ["GET"]),
        ],
        port=8083
    )
    # make request for import
    response = client.post(
        "/import/external",
        json=minimal_request_body | {"build": {"configuration": "-"}}
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    json = wait_for_report(client, response.json["value"])

    assert not json["data"]["success"]
    assert len(json["log"]["ERROR"]) == 2
    assert any("Builder returned with" in msg["body"] for msg in json["log"]["ERROR"])
    assert any("404" in msg["body"] for msg in json["log"]["ERROR"])
    assert any("did not build" in msg["body"] for msg in json["log"]["ERROR"])



@pytest.mark.parametrize(
    ("request_body_path", "new_value", "expected_status"),
    [
        (
            [], None, 201
        ),
        (
            ["import", "unknown"], None, 400
        ),
        (
            ["import", "args", "number"], "string", 422
        ),
        (
            ["unknown"], 0, 400
        ),
        (
            ["build"], 0, 422
        ),
        (
            ["validation"], 0, 422
        ),
        (
            ["callbackUrl"], "no-url", 422
        ),
    ],
    ids=[
        "good", "import-bad", "bad-plugin-arg", "unknown", "build-bad",
        "validation-bad", "callbackUrl",
    ]
)
def test_import_handlers(
    client, minimal_request_body,
    request_body_path, new_value, expected_status
):
    """
    Test correct application of handlers in /import/external-POST endpoint.
    """

    def set_inner_dict(in_, keys, value):
        if len(keys) == 0:
            return
        if len(keys) == 1:
            in_[keys[0]] = value
            return
        return set_inner_dict(in_[keys[0]], keys[1:], value)
    set_inner_dict(minimal_request_body, request_body_path, new_value)

    # make request for import
    response = client.post(
        "/import/external",
        json=minimal_request_body
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200
    print(response.data)
    assert response.status_code == expected_status


def test_import_plugin_progress(testing_config, client):
    """Test availability of plugin-progress during import."""
    duration = 0.01

    class TestPlugin(Interface):
        """Fake plugin"""
        _NAME = "test"
        _DESCRIPTION = "Test Plugin"
        _DEPENDENCIES = []
        _SIGNATURE = Signature()

        def get(self, **kwargs) -> PluginResult:
            result = PluginResult()
            for i in range(20):
                sleep(duration)
                self.set_progress(numeric=i)
            return result

    class Config(testing_config):
        SUPPORTED_PLUGINS = {TestPlugin.name: TestPlugin}

    client = app_factory(Config()).test_client()
    # make request for import
    response = client.post(
        "/import/external",
        json={"import": {"plugin": TestPlugin.name, "args": {}}}
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    token = response.json["value"]

    # wait for report to contain plugin-child
    max_sleep = 250
    c_sleep = 0
    while c_sleep < max_sleep:
        sleep(2*duration)
        response = client.get(
            f"/report?token={token}"
        )
        if "children" in response.json and len(response.json["children"]) > 0:
            break
        c_sleep = c_sleep + 1

    assert "children" in response.json

    # check for increasing numeric progress
    sleep(2*duration)
    response = client.get(f"/report?token={token}")
    value1 = response.json["children"]["0@test-plugin"]["progress"]["numeric"]
    sleep(2*duration)
    response = client.get(f"/report?token={token}")
    value2 = response.json["children"]["0@test-plugin"]["progress"]["numeric"]
    print(f"progress: {value2} > progress: {value1}")
    assert value2 > value1


def test_import_abort(
    minimal_request_body, client, run_service, file_storage
):
    """Test of /import/external-endpoint with build and abort."""

    report_file = file_storage / str(uuid4())
    delete_file = file_storage / str(uuid4())
    assert not report_file.exists()
    assert not delete_file.exists()

    # use first call for report as marker to abort now
    # (builder is waiting for validator)
    def external_report():
        report_file.touch()
        return jsonify({"intermediate": "data"}), 503

    # use as marker abort request has been made
    def external_abort():
        delete_file.touch()
        return Response("OK", mimetype="text/plain", status=200)

    # setup fake object validator
    run_service(
        routes=[
            ("/build", lambda: (jsonify(value="abcdef", expires=False), 201), ["POST"]),
            ("/build", external_abort, ["DELETE"]),
            ("/report", external_report, ["GET"]),
        ],
        port=8083
    )

    # make request for import
    token = client.post(
        "/import/external",
        json=minimal_request_body | {"build": {"configuration": "-"}}
    ).json["value"]
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    # wait until job is ready to be aborted
    time0 = time()
    while not report_file.exists() and time() - time0 < 2:
        sleep(0.01)
    assert report_file.exists()
    sleep(0.1)

    assert client.delete(
        f"/import?token={token}",
        json={"reason": "test abort", "origin": "pytest-runner"}
    ).status_code == 200
    assert delete_file.exists()
    report = client.get(f"/report?token={token}").json
    assert report["progress"]["status"] == "aborted"
    assert "Received SIGKILL" in str(report["log"])
    assert "Aborting child" in str(report["log"])
    assert "ie0@ip_builder" in report["children"]
    assert report["children"]["ie0@ip_builder"] == {"intermediate": "data"}
