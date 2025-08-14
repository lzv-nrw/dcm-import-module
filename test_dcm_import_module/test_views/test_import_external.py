"""
Test module for the `dcm_import_module/views/import_external.py`.
"""

from time import sleep, time
from uuid import uuid4

import pytest
from flask import jsonify, request as flask_request, Response
from dcm_common import LoggingContext as Context

from dcm_import_module import app_factory
from dcm_import_module.plugins import OAIPMHPlugin, OAIPMHPlugin2


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


def test_import_full(
    testing_config, minimal_request_body, client, wait_for_report, run_service,
    fake_build_report, fake_builder_service
):
    """
    Test of POST-/import/external-endpoint with build and validation for
    multiple IEs.
    """

    run_service(
        app=fake_builder_service,
        port=8081
    )
    run_service(
        app=fake_builder_service,
        port=8082
    )

    # make request for import
    minimal_request_body["import"]["args"]["number"] = 2
    response = client.post(
        "/import/external",
        json=minimal_request_body
        | {
            "build": {
                "mappingPlugin": {"plugin": "oai-mapper", "args": {}},
            },
            "objectValidation": {"plugins": {}}
        },
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200
    assert response.status_code == 201
    assert response.mimetype == "application/json"
    assert "value" in response.json

    json = wait_for_report(client, response.json["value"])

    assert json["data"]["success"]
    assert len(json["data"]["IEs"]) == 2
    assert (
        testing_config().FS_MOUNT_POINT / json["data"]["IEs"]["ie0"]["path"]
    ).is_dir()
    assert len(json["data"]["IPs"]) == 2
    assert len(json["children"]) == 4
    assert all(
        id_ in json["children"]
        for id_ in [
            "ip0@ip_builder",
            "ip0@object_validator",
            "ip1@ip_builder",
            "ip1@object_validator",
        ]
    )
    assert set(json["data"]["IPs"]["ip0"]["logId"]) == {
        "ip0@ip_builder",
        "ip0@object_validator",
    }
    assert set(json["data"]["IPs"]["ip1"]["logId"]) == {
        "ip1@ip_builder",
        "ip1@object_validator",
    }
    assert fake_build_report \
        == json["children"][json["data"]["IPs"]["ip0"]["logId"][0]]
    assert fake_build_report \
        == json["children"][json["data"]["IPs"]["ip1"]["logId"][0]]


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


@pytest.mark.parametrize(
    ("plugin", "transfer_url_info"),
    [
        (OAIPMHPlugin, {"regex": "asd"}),
        (OAIPMHPlugin2, [{"regex": "asd"}]),
    ],
    ids=["OAIPMHPlugin", "OAIPMHPlugin2"]
)
def test_timeout_of_source_system(
    testing_config, wait_for_report, run_service, plugin, transfer_url_info
):
    """Test import behavior when source system times out."""

    class ThisConfig(testing_config):
        SOURCE_SYSTEM_TIMEOUT = 0.1
        SOURCE_SYSTEM_TIMEOUT_RETRIES = 1
        SOURCE_SYSTEM_TIMEOUT_RETRY_INTERVAL = 1
        SUPPORTED_PLUGINS = [plugin]

    client = app_factory(ThisConfig(), block=True).test_client()
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
                "plugin": plugin.name,
                "args": {
                    "transfer_url_info": transfer_url_info,
                    "base_url": "http://localhost:8082/get",
                    "metadata_prefix": ""
                }
            },
        }
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200
    json = wait_for_report(client, response.json["value"])

    assert not json["data"]["success"]
    assert Context.ERROR.name in json["log"]
    assert "Timeout" in str(json["log"])


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
    del fake_build_report["data"]["valid"]
    fake_build_report["data"]["success"] = False
    fake_build_report["log"] = {
        "ERROR": [
            {
                "datetime": "2024-08-09T12:15:10+00:00",
                "origin": "IP Builder",
                "body": "Some error",
            },
        ]
    }
    # Run the IP Builder service
    run_service(
        routes=[
            ("/build", lambda: (jsonify(value="abcdef", expires=False), 201), ["POST"]),
            ("/report", lambda: (jsonify(**fake_build_report), 200), ["GET"]),
        ],
        port=8081
    )

    # make request for import
    response = client.post(
        "/import/external",
        json=minimal_request_body
        | {
            "build": {
                "mappingPlugin": {"plugin": "oai-mapper", "args": {}},
            }
        },
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    json = wait_for_report(client, response.json["value"])

    assert not json["data"]["success"]
    assert len(json["log"]["ERROR"]) == 2
    assert len(json["data"]["IEs"]) == 1
    assert len(json["data"]["IPs"]) == 1
    assert "path" not in json["data"]["IPs"]
    assert "valid" not in json["data"]["IPs"]["ip0"]
    assert len(json["children"]) == 1
    assert "ip0@ip_builder" in json["children"]


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
        port=8081
    )

    # make request for import
    minimal_request_body["import"]["args"]["bad_ies"] = True
    minimal_request_body["import"]["args"]["number"] = 2
    response = client.post(
        "/import/external",
        json=minimal_request_body
        | {
            "build": {
                "mappingPlugin": {"plugin": "oai-mapper", "args": {}},
            }
        },
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    json = wait_for_report(client, response.json["value"])

    assert not json["data"]["success"]
    assert any("Skip building" in msg["body"] for msg in json["log"]["INFO"])
    assert len(json["data"]["IEs"]) == 2
    assert json["data"]["IEs"]["ie0"]["fetchedPayload"]
    assert not json["data"]["IEs"]["ie1"]["fetchedPayload"]
    assert json["data"]["IEs"]["ie0"]["IPIdentifier"] == "ip0"
    assert json["data"]["IEs"]["ie1"]["IPIdentifier"] == "ip1"
    assert len(json["data"]["IPs"]) == 2
    assert len(json["children"]) == 1
    assert "ip0@ip_builder" in json["children"]
    assert json["data"]["IPs"]["ip0"]["IEIdentifier"] == "ie0"
    assert "valid" not in json["data"]["IPs"]["ip1"]
    assert json["data"]["IPs"]["ip1"]["IEIdentifier"] == "ie1"


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
        port=8081
    )
    # make request for import
    response = client.post(
        "/import/external",
        json=minimal_request_body
        | {
            "build": {
                "mappingPlugin": {"plugin": "oai-mapper", "args": {}},
            }
        },
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    json = wait_for_report(client, response.json["value"])

    assert json["data"]["success"] is valid
    assert json["data"]["IPs"]["ip0"]["valid"] is valid
    assert len(json["data"]["IEs"]) == 1
    assert len(json["data"]["IPs"]) == 1


def test_arg_forwarding_to_ip_builder(
    client, minimal_request_body, wait_for_report,
    run_service, fake_build_report
):
    """
    Test whether arguments in "build" are forwarded to builder service
    correctly.
    """

    def post():
        fake_build_report["args"] = flask_request.json
        return (jsonify(value="abcdef", expires=False), 201)
    run_service(
        routes=[
            ("/build", post, ["POST"]),
            ("/report", lambda: (jsonify(**fake_build_report), 200), ["GET"]),
        ],
        port=8081
    )
    # make request for import
    extra_args = {"build": {"mappingPlugin": {"plugin": "a", "args": {}}}}
    response = client.post(
        "/import/external",
        json=minimal_request_body | extra_args
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    json = wait_for_report(client, response.json["value"])

    assert (
        json["children"]["ip0@ip_builder"]["args"]["build"]["mappingPlugin"]
        == extra_args["build"]["mappingPlugin"]
    )


def test_no_connection_to_ip_builder(
    client, minimal_request_body, wait_for_report
):
    """
    Test behavior of import when no connection to builder can be established.
    """
    # make request for import
    response = client.post(
        "/import/external",
        json=minimal_request_body
        | {
            "build": {
                "mappingPlugin": {"plugin": "oai-mapper", "args": {}},
            }
        },
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    json = wait_for_report(client, response.json["value"])

    assert not json["data"]["success"]
    assert len(json["log"]["ERROR"]) == 2
    assert any(
        "Cannot connect to service at" in msg["body"]
        for msg in json["log"]["ERROR"]
    )
    assert any(
        "Failed to build" in msg["body"] and "ie0" in msg["body"]
        for msg in json["log"]["ERROR"]
    )


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
        port=8081
    )
    # make request for import
    response = client.post(
        "/import/external",
        json=minimal_request_body
        | {
            "build": {
                "mappingPlugin": {"plugin": "oai-mapper", "args": {}},
            }
        },
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    json = wait_for_report(client, response.json["value"])

    assert not json["data"]["success"]
    assert len(json["log"]["ERROR"]) == 2
    assert any(rejection_msg in msg["body"] for msg in json["log"]["ERROR"])
    assert any(str(rejection_status) in msg["body"] for msg in json["log"]["ERROR"])
    assert any("Failed to build" in msg["body"] for msg in json["log"]["ERROR"])


def test_timeout_of_ip_builder(
    testing_config, minimal_request_body, wait_for_report, run_service,
    fake_build_report
):
    """Test import behavior when builder times out."""

    class ThisConfig(testing_config):
        IP_BUILDER_JOB_TIMEOUT = 0.25
    client = app_factory(ThisConfig(), block=True).test_client()
    run_service(
        routes=[
            ("/build", lambda: (jsonify(value="abcdef", expires=False), 201), ["POST"]),
            ("/report", lambda: (jsonify(fake_build_report), 503), ["GET"]),
        ],
        port=8081
    )
    # make request for import
    response = client.post(
        "/import/external",
        json=minimal_request_body
        | {
            "build": {
                "mappingPlugin": {"plugin": "oai-mapper", "args": {}},
            }
        },
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    json = wait_for_report(client, response.json["value"])

    assert not json["data"]["success"]
    assert len(json["log"]["ERROR"]) == 2
    assert any("has timed out" in msg["body"] for msg in json["log"]["ERROR"])
    assert any(
        "Failed to build" in msg["body"] for msg in json["log"]["ERROR"]
    )


def test_unknown_report_from_ip_builder(
    client, minimal_request_body, wait_for_report, run_service
):
    """Test import behavior when builder 'forgets' report."""

    run_service(
        routes=[
            ("/build", lambda: (jsonify(value="abcdef", expires=False), 201), ["POST"]),
            ("/report", lambda: Response("What?", 404), ["GET"]),
        ],
        port=8081
    )
    # make request for import
    response = client.post(
        "/import/external",
        json=minimal_request_body
        | {
            "build": {
                "mappingPlugin": {"plugin": "oai-mapper", "args": {}},
            }
        },
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200

    json = wait_for_report(client, response.json["value"])

    assert not json["data"]["success"]
    assert len(json["log"]["ERROR"]) == 2
    assert any(
        "responded with an unknown error" in msg["body"]
        and "What?" in msg["body"]
        and "404" in msg["body"]
        for msg in json["log"]["ERROR"]
    )


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
            ["objectValidation"], 0, 422
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
        port=8081
    )

    # make request for import
    token = client.post(
        "/import/external",
        json=minimal_request_body
        | {
            "build": {
                "mappingPlugin": {"plugin": "oai-mapper", "args": {}},
            }
        },
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
    assert "ip0@ip_builder" in report["children"]
    assert report["children"]["ip0@ip_builder"] == {"intermediate": "data"}
