"""
Test module for the `dcm_import_module/views/import_ies.py`.
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
            "args": {"number": 1, "randomize": False},
        }
    }


def test_import_full(
    testing_config,
    minimal_request_body,
    run_service,
    fake_build_report,
    fake_builder_service,
):
    """
    Test of POST-/import/ies-endpoint with build and validation for
    multiple IEs.
    """

    run_service(app=fake_builder_service, port=8081)
    run_service(app=fake_builder_service, port=8082)

    app = app_factory(testing_config())
    client = app.test_client()

    # make request for import
    minimal_request_body["import"]["args"]["number"] = 2
    response = client.post(
        "/import/ies",
        json=minimal_request_body
        | {
            "build": {
                "mappingPlugin": {"plugin": "oai-mapper", "args": {}},
            },
            "objectValidation": {"plugins": {}},
        },
    )
    assert response.status_code == 201
    assert response.mimetype == "application/json"
    assert "value" in response.json

    app.extensions["orchestra"].stop(stop_on_idle=True)
    report = client.get(f"/report?token={response.json['value']}").json

    assert report["data"]["success"]
    assert len(report["data"]["IEs"]) == 2
    assert (
        testing_config().FS_MOUNT_POINT / report["data"]["IEs"]["ie0"]["path"]
    ).is_dir()
    assert len(report["data"]["IPs"]) == 2
    assert len(report["children"]) == 4
    assert all(
        id_ in report["children"]
        for id_ in [
            "ip0@ip_builder",
            "ip0@object_validator",
            "ip1@ip_builder",
            "ip1@object_validator",
        ]
    )
    assert set(report["data"]["IPs"]["ip0"]["logId"]) == {
        "ip0@ip_builder",
        "ip0@object_validator",
    }
    assert set(report["data"]["IPs"]["ip1"]["logId"]) == {
        "ip1@ip_builder",
        "ip1@object_validator",
    }
    assert (
        fake_build_report
        == report["children"][report["data"]["IPs"]["ip0"]["logId"][0]]
    )
    assert (
        fake_build_report
        == report["children"][report["data"]["IPs"]["ip1"]["logId"][0]]
    )


def test_import_only_ie(testing_config, minimal_request_body):
    """Minimal test of /import/ies-endpoint."""

    app = app_factory(testing_config())
    client = app.test_client()

    # make request for import
    response = client.post("/import/ies", json=minimal_request_body)

    app.extensions["orchestra"].stop(stop_on_idle=True)
    report = client.get(f"/report?token={response.json['value']}").json

    assert report["data"]["success"]
    assert (
        testing_config().FS_MOUNT_POINT / report["data"]["IEs"]["ie0"]["path"]
    ).is_dir()
    assert report["data"]["IEs"]["ie0"]["IPIdentifier"] is None
    assert any("Skip building" in msg["body"] for msg in report["log"]["INFO"])
    assert "IPs" not in report["data"]


def test_import_empty(testing_config, minimal_request_body):
    """Test of /import/ies-endpoint if not IEs are generated."""

    app = app_factory(testing_config())
    client = app.test_client()

    # make request for import
    minimal_request_body["import"]["args"]["number"] = 0
    response = client.post("/import/ies", json=minimal_request_body)

    app.extensions["orchestra"].stop(stop_on_idle=True)
    report = client.get(f"/report?token={response.json['value']}").json

    assert report["data"]["success"]
    assert any(
        "List of IEs is empty" in msg["body"] for msg in report["log"]["INFO"]
    )
    assert len(report["data"]["IEs"]) == 0
    assert "IPs" not in report["data"]


@pytest.mark.parametrize(
    ("plugin", "transfer_url_info"),
    [
        (OAIPMHPlugin, {"regex": "asd"}),
        (OAIPMHPlugin2, [{"regex": "asd"}]),
    ],
    ids=["OAIPMHPlugin", "OAIPMHPlugin2"],
)
def test_timeout_of_source_system(
    testing_config, run_service, plugin, transfer_url_info
):
    """Test import behavior when source system times out."""

    class ThisConfig(testing_config):
        SOURCE_SYSTEM_TIMEOUT = 0.1
        SOURCE_SYSTEM_TIMEOUT_RETRIES = 1
        SOURCE_SYSTEM_TIMEOUT_RETRY_INTERVAL = 1
        SUPPORTED_PLUGINS = [plugin]

    app = app_factory(ThisConfig())
    client = app.test_client()

    # fake IP Builder
    run_service(
        routes=[
            (
                "/build",
                lambda: (jsonify(value="abcdef", expires=False), 201),
                ["POST"],
            ),
            ("/report", lambda: Response("No", 503), ["GET"]),
        ],
        port=8083,
    )

    # fake source system
    def timeout():
        sleep(2 * ThisConfig().SOURCE_SYSTEM_TIMEOUT)

    run_service(routes=[("/get", timeout, ["GET"])], port=8082)

    # make request for import
    response = client.post(
        "/import/ies",
        json={
            "import": {
                "plugin": plugin.name,
                "args": {
                    "transfer_url_info": transfer_url_info,
                    "base_url": "http://localhost:8082/get",
                    "metadata_prefix": "",
                },
            },
        },
    )

    app.extensions["orchestra"].stop(stop_on_idle=True)
    report = client.get(f"/report?token={response.json['value']}").json

    assert not report["data"]["success"]
    assert Context.ERROR.name in report["log"]
    assert "Timeout" in str(report["log"])


def test_import_no_path(
    minimal_request_body, testing_config, run_service, fake_build_report
):
    """
    Test of /import/ies-endpoint if no path is returned by the IP
    Builder.
    """

    app = app_factory(testing_config())
    client = app.test_client()

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
            (
                "/build",
                lambda: (jsonify(value="abcdef", expires=False), 201),
                ["POST"],
            ),
            ("/report", lambda: (jsonify(**fake_build_report), 200), ["GET"]),
        ],
        port=8081,
    )

    # make request for import
    response = client.post(
        "/import/ies",
        json=minimal_request_body
        | {
            "build": {
                "mappingPlugin": {"plugin": "oai-mapper", "args": {}},
            }
        },
    )

    app.extensions["orchestra"].stop(stop_on_idle=True)
    report = client.get(f"/report?token={response.json['value']}").json

    assert not report["data"]["success"]
    assert len(report["log"]["ERROR"]) == 2
    assert len(report["data"]["IEs"]) == 1
    assert len(report["data"]["IPs"]) == 1
    assert "path" not in report["data"]["IPs"]
    assert "valid" not in report["data"]["IPs"]["ip0"]
    assert len(report["children"]) == 1
    assert "ip0@ip_builder" in report["children"]


def test_missing_payload_in_ie(
    minimal_request_body, testing_config, run_service, fake_builder_service
):
    """
    Test of /import/ies-endpoint where IE is not complete (by
    faking plugin).
    """

    app = app_factory(testing_config())
    client = app.test_client()

    run_service(app=fake_builder_service, port=8081)

    # make request for import
    minimal_request_body["import"]["args"]["bad_ies"] = True
    minimal_request_body["import"]["args"]["number"] = 2
    response = client.post(
        "/import/ies",
        json=minimal_request_body
        | {
            "build": {
                "mappingPlugin": {"plugin": "oai-mapper", "args": {}},
            }
        },
    )

    app.extensions["orchestra"].stop(stop_on_idle=True)
    report = client.get(f"/report?token={response.json['value']}").json

    assert not report["data"]["success"]
    assert any("Skip building" in msg["body"] for msg in report["log"]["INFO"])
    assert len(report["data"]["IEs"]) == 2
    assert report["data"]["IEs"]["ie0"]["fetchedPayload"]
    assert not report["data"]["IEs"]["ie1"]["fetchedPayload"]
    assert report["data"]["IEs"]["ie0"]["IPIdentifier"] == "ip0"
    assert report["data"]["IEs"]["ie1"]["IPIdentifier"] == "ip1"
    assert len(report["data"]["IPs"]) == 2
    assert len(report["children"]) == 1
    assert "ip0@ip_builder" in report["children"]
    assert report["data"]["IPs"]["ip0"]["IEIdentifier"] == "ie0"
    assert "valid" not in report["data"]["IPs"]["ip1"]
    assert report["data"]["IPs"]["ip1"]["IEIdentifier"] == "ie1"


@pytest.mark.parametrize("valid", [True, False], ids=["valid", "invalid"])
def test_processing_of_invalid_ip(
    minimal_request_body, testing_config, run_service, fake_build_report, valid
):
    """
    Test of /import/ies-endpoint where builder returns with invalid
    flag.
    """

    app = app_factory(testing_config())
    client = app.test_client()

    fake_build_report["data"]["valid"] = valid
    run_service(
        routes=[
            (
                "/build",
                lambda: (jsonify(value="abcdef", expires=False), 201),
                ["POST"],
            ),
            ("/report", lambda: (jsonify(**fake_build_report), 200), ["GET"]),
        ],
        port=8081,
    )

    # make request for import
    response = client.post(
        "/import/ies",
        json=minimal_request_body
        | {
            "build": {
                "mappingPlugin": {"plugin": "oai-mapper", "args": {}},
            }
        },
    )

    app.extensions["orchestra"].stop(stop_on_idle=True)
    report = client.get(f"/report?token={response.json['value']}").json

    assert report["data"]["success"] is valid
    assert report["data"]["IPs"]["ip0"]["valid"] is valid
    assert len(report["data"]["IEs"]) == 1
    assert len(report["data"]["IPs"]) == 1


def test_arg_forwarding_to_ip_builder(
    minimal_request_body, testing_config, run_service, fake_build_report
):
    """
    Test whether arguments in "build" are forwarded to builder service
    correctly.
    """

    app = app_factory(testing_config())
    client = app.test_client()

    def post():
        fake_build_report["args"] = flask_request.json
        return (jsonify(value="abcdef", expires=False), 201)

    run_service(
        routes=[
            ("/build", post, ["POST"]),
            ("/report", lambda: (jsonify(**fake_build_report), 200), ["GET"]),
        ],
        port=8081,
    )
    # make request for import
    extra_args = {"build": {"mappingPlugin": {"plugin": "a", "args": {}}}}
    response = client.post(
        "/import/ies", json=minimal_request_body | extra_args
    )

    app.extensions["orchestra"].stop(stop_on_idle=True)
    report = client.get(f"/report?token={response.json['value']}").json

    assert (
        report["children"]["ip0@ip_builder"]["args"]["build"]["mappingPlugin"]
        == extra_args["build"]["mappingPlugin"]
    )


def test_no_connection_to_ip_builder(minimal_request_body, testing_config):
    """
    Test behavior of import when no connection to builder can be established.
    """

    app = app_factory(testing_config())
    client = app.test_client()

    # make request for import
    response = client.post(
        "/import/ies",
        json=minimal_request_body
        | {
            "build": {
                "mappingPlugin": {"plugin": "oai-mapper", "args": {}},
            }
        },
    )

    app.extensions["orchestra"].stop(stop_on_idle=True)
    report = client.get(f"/report?token={response.json['value']}").json

    assert not report["data"]["success"]
    assert len(report["log"]["ERROR"]) == 2
    assert any(
        "Cannot connect to service at" in msg["body"]
        for msg in report["log"]["ERROR"]
    )
    assert any(
        "Failed to build" in msg["body"] and "ie0" in msg["body"]
        for msg in report["log"]["ERROR"]
    )


def test_rejection_by_ip_builder(
    minimal_request_body, testing_config, run_service
):
    """
    Test behavior of import when builder rejects any request.
    """

    app = app_factory(testing_config())
    client = app.test_client()

    rejection_msg = "No, will not process something like that."
    rejection_status = 422
    run_service(
        routes=[
            (
                "/build",
                lambda: Response(rejection_msg, status=rejection_status),
                ["POST"],
            ),
        ],
        port=8081,
    )
    # make request for import
    response = client.post(
        "/import/ies",
        json=minimal_request_body
        | {
            "build": {
                "mappingPlugin": {"plugin": "oai-mapper", "args": {}},
            }
        },
    )

    app.extensions["orchestra"].stop(stop_on_idle=True)
    report = client.get(f"/report?token={response.json['value']}").json

    assert not report["data"]["success"]
    assert len(report["log"]["ERROR"]) == 2
    assert any(rejection_msg in msg["body"] for msg in report["log"]["ERROR"])
    assert any(
        str(rejection_status) in msg["body"] for msg in report["log"]["ERROR"]
    )
    assert any(
        "Failed to build" in msg["body"] for msg in report["log"]["ERROR"]
    )


def test_timeout_of_ip_builder(
    testing_config, minimal_request_body, run_service, fake_build_report
):
    """Test import behavior when builder times out."""

    class ThisConfig(testing_config):
        SERVICE_TIMEOUT = 0.25

    app = app_factory(ThisConfig())
    client = app.test_client()

    run_service(
        routes=[
            (
                "/build",
                lambda: (jsonify(value="abcdef", expires=False), 201),
                ["POST"],
            ),
            ("/report", lambda: (jsonify(fake_build_report), 503), ["GET"]),
        ],
        port=8081,
    )

    # make request for import
    response = client.post(
        "/import/ies",
        json=minimal_request_body
        | {
            "build": {
                "mappingPlugin": {"plugin": "oai-mapper", "args": {}},
            }
        },
    )

    app.extensions["orchestra"].stop(stop_on_idle=True)
    report = client.get(f"/report?token={response.json['value']}").json

    assert not report["data"]["success"]
    assert len(report["log"]["ERROR"]) == 2
    assert any(
        "has timed out" in msg["body"] for msg in report["log"]["ERROR"]
    )
    assert any(
        "Failed to build" in msg["body"] for msg in report["log"]["ERROR"]
    )


def test_unknown_report_from_ip_builder(
    minimal_request_body, testing_config, run_service
):
    """Test import behavior when builder 'forgets' report."""

    app = app_factory(testing_config())
    client = app.test_client()

    run_service(
        routes=[
            (
                "/build",
                lambda: (jsonify(value="abcdef", expires=False), 201),
                ["POST"],
            ),
            ("/report", lambda: Response("What?", 404), ["GET"]),
        ],
        port=8081,
    )

    # make request for import
    response = client.post(
        "/import/ies",
        json=minimal_request_body
        | {
            "build": {
                "mappingPlugin": {"plugin": "oai-mapper", "args": {}},
            }
        },
    )

    app.extensions["orchestra"].stop(stop_on_idle=True)
    report = client.get(f"/report?token={response.json['value']}").json

    assert not report["data"]["success"]
    assert len(report["log"]["ERROR"]) == 2
    assert any(
        "responded with an unknown error" in msg["body"]
        and "What?" in msg["body"]
        and "404" in msg["body"]
        for msg in report["log"]["ERROR"]
    )


def test_import_abort(
    minimal_request_body, testing_config, run_service, file_storage
):
    """Test of /import/ies-endpoint with build and abort."""

    app = app_factory(testing_config())
    client = app.test_client()

    report_file = file_storage / str(uuid4())
    delete_file = file_storage / str(uuid4())
    assert not report_file.exists()
    assert not delete_file.exists()

    # use first call for report as indicator to abort now
    # (builder is waiting for validator)
    def external_report():
        report_file.touch()
        return jsonify({"intermediate": "data"}), 503

    # use as indicator that abort request has been made
    def external_abort():
        delete_file.touch()
        return Response("OK", mimetype="text/plain", status=200)

    # setup fake object validator
    run_service(
        routes=[
            (
                "/build",
                lambda: (jsonify(value="abcdef", expires=False), 201),
                ["POST"],
            ),
            ("/build", external_abort, ["DELETE"]),
            ("/report", external_report, ["GET"]),
        ],
        port=8081,
    )

    # make request for import
    token = client.post(
        "/import/ies",
        json=minimal_request_body
        | {
            "build": {
                "mappingPlugin": {"plugin": "oai-mapper", "args": {}},
            }
        },
    ).json["value"]

    # wait until job is ready to be aborted
    time0 = time()
    while not report_file.exists() and time() - time0 < 2:
        sleep(0.01)
    assert report_file.exists()
    sleep(0.1)
    assert (
        client.delete(
            f"/import?token={token}",
            json={"reason": "test abort", "origin": "pytest-runner"},
        ).status_code
        == 200
    )
    assert delete_file.exists()

    app.extensions["orchestra"].stop(stop_on_idle=True)
    report = client.get(f"/report?token={token}").json

    assert report["progress"]["status"] == "aborted"
    assert "ip0@ip_builder" in report["children"]
    assert report["children"]["ip0@ip_builder"] == {"intermediate": "data"}
