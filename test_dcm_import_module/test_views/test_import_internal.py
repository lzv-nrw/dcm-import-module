"""
Test module for the `dcm_import_module/views/import_internal.py`.
"""

from pathlib import Path
from time import sleep, time
from uuid import uuid4

import pytest
from flask import jsonify, Response
from dcm_common import LoggingContext as Context

from dcm_import_module import app_factory


@pytest.fixture(name="minimal_request_body")
def _minimal_request_body(create_fake_ip):
    hotfolder = Path(str(uuid4()))
    create_fake_ip(hotfolder / "ip0")
    return {
        "import": {
            "target": {"path": str(hotfolder)},
        }
    }


@pytest.fixture(name="create_fake_ip")
def _create_fake_ip(file_storage):
    """
    Returns function that can be used to generate a fake ip at a given
    location.
    """
    def create_fake_ip(path):
        _path = file_storage / path
        _path.mkdir(parents=True, exist_ok=False)
        (_path / "data").mkdir()
        (_path / "bagit.txt").touch()
    return create_fake_ip


def test_import_minimal(minimal_request_body, client, wait_for_report):
    """Minimal test of /import/internal-endpoint."""

    # make request for import
    response = client.post(
        "/import/internal",
        json=minimal_request_body
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200
    assert response.status_code == 201
    assert response.mimetype == "application/json"
    assert "value" in response.json

    json = wait_for_report(client, response.json["value"])

    assert json["data"]["success"]
    assert len(json["data"]["IPs"]) == 1
    assert "ip0" in json["data"]["IPs"]
    assert json["data"]["IPs"]["ip0"]["path"].startswith(
        minimal_request_body["import"]["target"]["path"]
    )


def test_import_with_validation(
    minimal_request_body, fake_builder_service, fake_validation_report,
    client, wait_for_report, run_service
):
    """Test of /import/internal-endpoint with validation."""

    run_service(
        app=fake_builder_service,
        port=8083
    )

    # make request for import
    response = client.post(
        "/import/internal",
        json=minimal_request_body | {
            "validation": {"modules": ["bagit_profile"]}
        }
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200
    json = wait_for_report(client, response.json["value"])

    assert json["data"]["success"]
    assert json["data"]["IPs"]["ip0"]["valid"]
    assert json["data"]["IPs"]["ip0"]["logId"] is not None
    assert json["data"]["IPs"]["ip0"]["logId"] in json["children"]
    assert len(json["children"]) == 1
    assert (
        json["children"][json["data"]["IPs"]["ip0"]["logId"]]
        == fake_validation_report
    )


def test_import_with_validation_fail(
    minimal_request_body, fake_builder_service_fail,
    client, wait_for_report, run_service
):
    """Test of /import/internal-endpoint with validation."""

    run_service(
        app=fake_builder_service_fail,
        port=8083
    )

    # make request for import
    response = client.post(
        "/import/internal",
        json=minimal_request_body | {
            "validation": {"modules": ["bagit_profile"]}
        }
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200
    json = wait_for_report(client, response.json["value"])

    assert not json["data"]["success"]
    assert not json["data"]["IPs"]["ip0"]["valid"]
    assert Context.ERROR.name in json["log"]


def test_import_batch_multiple(create_fake_ip, client, wait_for_report):
    """
    Test batch-import of multiple IPs via /import/internal-endpoint.
    """

    hotfolder = Path(str(uuid4()))
    create_fake_ip(hotfolder / "ip0")
    create_fake_ip(hotfolder / "ip1")
    # make request for import
    response = client.post(
        "/import/internal",
        json={
            "import": {
                "target": {"path": str(hotfolder)},
                "batch": True
            }
        }
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200
    json = wait_for_report(client, response.json["value"])

    assert json["data"]["success"]
    assert len(json["data"]["IPs"]) == 2
    assert "ip0" in json["data"]["IPs"]
    assert "ip1" in json["data"]["IPs"]


def test_import_no_batch(create_fake_ip, client, wait_for_report):
    """
    Test no-batch-import of single IP via /import/internal-endpoint.
    """

    hotfolder = Path(str(uuid4()))
    create_fake_ip(hotfolder / "ip0")
    # make request for import
    response = client.post(
        "/import/internal",
        json={
            "import": {
                "target": {"path": str(hotfolder / "ip0")},
                "batch": False
            }
        }
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200
    json = wait_for_report(client, response.json["value"])

    assert json["data"]["success"]
    assert len(json["data"]["IPs"]) == 1
    assert "ip0" in json["data"]["IPs"]


def test_import_builder_timeout(
    minimal_request_body, testing_config, wait_for_report, run_service
):
    """Test of /import/internal-endpoint with timeout of validation."""

    run_service(
        routes=[
            ("/validate/ip", lambda: (jsonify(value="abcdef", expires=False), 201), ["POST"]),
            ("/report", lambda: (jsonify({"host": "", "progress": {}}), 503), ["GET"]),
        ],
        port=8083
    )

    testing_config.IP_BUILDER_JOB_TIMEOUT = 0.001
    client = app_factory(testing_config()).test_client()

    # make request for import
    response = client.post(
        "/import/internal",
        json=minimal_request_body | {
            "validation": {"modules": ["bagit_profile"]}
        }
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200
    json = wait_for_report(client, response.json["value"])

    assert not json["data"]["success"]
    assert not json["data"]["IPs"]["ip0"]["valid"]
    assert Context.ERROR.name in json["log"]


def test_import_builder_unavailable(
    minimal_request_body, client, wait_for_report, run_service
):
    """Test of /import/internal-endpoint with timeout of validation."""

    # make request for import
    response = client.post(
        "/import/internal",
        json=minimal_request_body | {
            "validation": {"modules": ["bagit_profile"]}
        }
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200
    json = wait_for_report(client, response.json["value"])

    assert not json["data"]["success"]
    assert not json["data"]["IPs"]["ip0"]["valid"]
    assert Context.ERROR.name in json["log"]


def test_import_empty(file_storage, client, wait_for_report):
    """Test of /import/internal-endpoint for empty hotfolder."""

    hotfolder = str(uuid4())
    (file_storage / hotfolder).mkdir()
    # make request for import
    response = client.post(
        "/import/internal",
        json={"import": {"target": {"path": hotfolder}}}
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200
    json = wait_for_report(client, response.json["value"])

    assert json["data"]["success"]
    assert len(json["data"]["IPs"]) == 0


def test_import_non_IPs(file_storage, create_fake_ip, client, wait_for_report):
    """
    Test of /import/internal-endpoint for hotfolder containing non-IPs.
    """

    hotfolder = Path(str(uuid4()))
    (file_storage / hotfolder).mkdir()
    create_fake_ip(hotfolder / "ip0")
    create_fake_ip(hotfolder / "no-ip")
    (file_storage / hotfolder / "no-ip" / "bagit.txt").unlink()
    (file_storage / hotfolder / "some-file").touch()

    # make request for import
    response = client.post(
        "/import/internal",
        json={"import": {"target": {"path": str(hotfolder)}}}
    )
    assert client.put("/orchestration?until-idle", json={}).status_code == 200
    json = wait_for_report(client, response.json["value"])

    assert json["data"]["success"]
    assert len(json["data"]["IPs"]) == 1
    assert "ip0" in json["data"]["IPs"]


def test_import_abort(
    minimal_request_body, client, run_service, file_storage, fake_build_report
):
    """Test of /import/internal-endpoint with build and abort."""

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
            ("/validate/ip", lambda: (jsonify(value="abcdef", expires=False), 201), ["POST"]),
            ("/validate", external_abort, ["DELETE"]),
            ("/report", external_report, ["GET"]),
        ],
        port=8083
    )

    # make request for import
    token = client.post(
        "/import/internal",
        json=minimal_request_body | {"validation": {"modules": ["module1"]}}
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
