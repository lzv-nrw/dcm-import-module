"""
Test module for the `dcm_import_module/views/import_ips.py`.
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


def test_import_minimal(minimal_request_body, testing_config):
    """Minimal test of /import/ips-endpoint."""

    app = app_factory(testing_config())
    client = app.test_client()

    # make request for import
    response = client.post("/import/ips", json=minimal_request_body)
    assert response.status_code == 201
    assert response.mimetype == "application/json"
    assert "value" in response.json

    app.extensions["orchestra"].stop(stop_on_idle=True)
    report = client.get(f"/report?token={response.json['value']}").json

    assert report["data"]["success"]
    assert len(report["data"]["IPs"]) == 1
    assert "ip0" in report["data"]["IPs"]
    assert report["data"]["IPs"]["ip0"]["path"].startswith(
        minimal_request_body["import"]["target"]["path"]
    )


def test_import_with_spec_validation(
    minimal_request_body,
    fake_builder_service,
    fake_validation_report,
    testing_config,
    run_service,
):
    """Test of /import/ips-endpoint with spec-validation."""

    app = app_factory(testing_config())
    client = app.test_client()

    run_service(app=fake_builder_service, port=8081)

    # make request for import
    response = client.post(
        "/import/ips",
        json=minimal_request_body
        | {
            "specificationValidation": {
                "BagItProfile": "bagit_profiles/dcm_bagit_profile_v1.0.0.json",
            }
        },
    )

    app.extensions["orchestra"].stop(stop_on_idle=True)
    report = client.get(f"/report?token={response.json['value']}").json

    assert report["data"]["success"]
    assert report["data"]["IPs"]["ip0"]["valid"]
    assert report["data"]["IPs"]["ip0"]["logId"] is not None
    assert all(
        log_id in report["children"]
        for log_id in report["data"]["IPs"]["ip0"]["logId"]
    )
    assert len(report["children"]) == 1
    assert (
        report["children"][report["data"]["IPs"]["ip0"]["logId"][0]]
        == fake_validation_report
    )


def test_import_with_obj_validation(
    minimal_request_body, fake_builder_service, testing_config, run_service
):
    """Test of /import/ips-endpoint with object-validation."""

    app = app_factory(testing_config())
    client = app.test_client()

    run_service(app=fake_builder_service, port=8082)

    # make request for import
    response = client.post(
        "/import/ips",
        json=minimal_request_body
        | {
            "objectValidation": {
                "plugins": {},
            }
        },
    )

    app.extensions["orchestra"].stop(stop_on_idle=True)
    report = client.get(f"/report?token={response.json['value']}").json

    assert report["data"]["success"]
    assert report["data"]["IPs"]["ip0"]["valid"]
    assert len(report["children"]) == 1


def test_import_with_validation_fail(
    minimal_request_body,
    fake_builder_service_fail,
    testing_config,
    run_service,
):
    """Test of /import/ips-endpoint with validation."""

    app = app_factory(testing_config())
    client = app.test_client()

    run_service(app=fake_builder_service_fail, port=8081)

    # make request for import
    response = client.post(
        "/import/ips",
        json=minimal_request_body
        | {
            "specificationValidation": {
                "BagItProfile": "bagit_profiles/dcm_bagit_profile_v1.0.0.json",
            }
        },
    )

    app.extensions["orchestra"].stop(stop_on_idle=True)
    report = client.get(f"/report?token={response.json['value']}").json

    assert not report["data"]["success"]
    assert not report["data"]["IPs"]["ip0"]["valid"]
    assert Context.ERROR.name in report["log"]


def test_import_batch_multiple(create_fake_ip, testing_config):
    """
    Test batch-import of multiple IPs via /import/ips-endpoint.
    """

    app = app_factory(testing_config())
    client = app.test_client()

    hotfolder = Path(str(uuid4()))
    create_fake_ip(hotfolder / "ip0")
    create_fake_ip(hotfolder / "ip1")
    # make request for import
    response = client.post(
        "/import/ips",
        json={"import": {"target": {"path": str(hotfolder)}, "batch": True}},
    )

    app.extensions["orchestra"].stop(stop_on_idle=True)
    report = client.get(f"/report?token={response.json['value']}").json

    assert report["data"]["success"]
    assert len(report["data"]["IPs"]) == 2
    assert "ip0" in report["data"]["IPs"]
    assert "ip1" in report["data"]["IPs"]


def test_import_no_batch(create_fake_ip, testing_config):
    """
    Test no-batch-import of single IP via /import/ips-endpoint.
    """

    app = app_factory(testing_config())
    client = app.test_client()

    hotfolder = Path(str(uuid4()))
    create_fake_ip(hotfolder / "ip0")
    # make request for import
    response = client.post(
        "/import/ips",
        json={
            "import": {
                "target": {"path": str(hotfolder / "ip0")},
                "batch": False,
            }
        },
    )

    app.extensions["orchestra"].stop(stop_on_idle=True)
    report = client.get(f"/report?token={response.json['value']}").json

    assert report["data"]["success"]
    assert len(report["data"]["IPs"]) == 1
    assert "ip0" in report["data"]["IPs"]


def test_import_builder_timeout(
    minimal_request_body, testing_config, run_service
):
    """Test of /import/ips-endpoint with timeout of validation."""

    class ThisConfig(testing_config):
        SERVICE_TIMEOUT = 0.001

    app = app_factory(ThisConfig())
    client = app.test_client()

    run_service(
        routes=[
            (
                "/validate",
                lambda: (jsonify(value="abcdef", expires=False), 201),
                ["POST"],
            ),
            (
                "/report",
                lambda: (jsonify({"host": "", "progress": {}}), 503),
                ["GET"],
            ),
        ],
        port=8081,
    )

    # make request for import
    response = client.post(
        "/import/ips",
        json=minimal_request_body
        | {
            "specificationValidation": {
                "BagItProfile": "bagit_profiles/dcm_bagit_profile_v1.0.0.json",
            }
        },
    )

    app.extensions["orchestra"].stop(stop_on_idle=True)
    report = client.get(f"/report?token={response.json['value']}").json

    assert not report["data"]["success"]
    assert not report["data"]["IPs"]["ip0"]["valid"]
    assert Context.ERROR.name in report["log"]


def test_import_builder_unavailable(
    minimal_request_body, testing_config, run_service
):
    """Test of /import/ips-endpoint with timeout of validation."""

    app = app_factory(testing_config())
    client = app.test_client()

    # make request for import
    response = client.post(
        "/import/ips",
        json=minimal_request_body
        | {
            "specificationValidation": {
                "BagItProfile": "bagit_profiles/dcm_bagit_profile_v1.0.0.json",
            }
        },
    )

    app.extensions["orchestra"].stop(stop_on_idle=True)
    report = client.get(f"/report?token={response.json['value']}").json

    assert not report["data"]["success"]
    assert not report["data"]["IPs"]["ip0"]["valid"]
    assert Context.ERROR.name in report["log"]


def test_import_empty(file_storage, testing_config):
    """Test of /import/ips-endpoint for empty hotfolder."""

    app = app_factory(testing_config())
    client = app.test_client()

    hotfolder = str(uuid4())
    (file_storage / hotfolder).mkdir()
    # make request for import
    response = client.post(
        "/import/ips", json={"import": {"target": {"path": hotfolder}}}
    )

    app.extensions["orchestra"].stop(stop_on_idle=True)
    report = client.get(f"/report?token={response.json['value']}").json

    assert report["data"]["success"]
    assert len(report["data"]["IPs"]) == 0


def test_import_non_ips(file_storage, create_fake_ip, testing_config):
    """
    Test of /import/ips-endpoint for hotfolder containing non-IPs.
    """

    app = app_factory(testing_config())
    client = app.test_client()

    hotfolder = Path(str(uuid4()))
    (file_storage / hotfolder).mkdir()
    create_fake_ip(hotfolder / "ip0")
    create_fake_ip(hotfolder / "no-ip")
    (file_storage / hotfolder / "no-ip" / "bagit.txt").unlink()
    (file_storage / hotfolder / "some-file").touch()

    # make request for import
    response = client.post(
        "/import/ips",
        json={"import": {"target": {"path": str(hotfolder)}}},
    )

    app.extensions["orchestra"].stop(stop_on_idle=True)
    report = client.get(f"/report?token={response.json['value']}").json

    assert report["data"]["success"]
    assert len(report["data"]["IPs"]) == 1
    assert "ip0" in report["data"]["IPs"]


def test_import_abort(
    minimal_request_body,
    testing_config,
    run_service,
    file_storage,
    fake_build_report,
):
    """Test of /import/ips-endpoint with build and abort."""

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
                "/validate",
                lambda: (jsonify(value="abcdef", expires=False), 201),
                ["POST"],
            ),
            ("/validate", external_abort, ["DELETE"]),
            ("/report", external_report, ["GET"]),
        ],
        port=8081,
    )

    # make request for import
    token = client.post(
        "/import/ips",
        json=minimal_request_body
        | {
            "specificationValidation": {
                "BagItProfile": "bagit_profiles/dcm_bagit_profile_v1.0.0.json",
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


@pytest.mark.parametrize(
    ("max_records", "expected_ips"),
    [
        (1, 1),
        (2, 2),
        (3, 3),
        (4, 3),
    ],
)
def test_import_test(
    create_fake_ip,
    testing_config,
    max_records,
    expected_ips,
):
    """
    Test test-import of multiple IPs via /import/ips-endpoint.
    """

    class ThisConfig(testing_config):
        IMPORT_TEST_VOLUME = max_records

    app = app_factory(ThisConfig())
    client = app.test_client()

    hotfolder = Path(str(uuid4()))
    create_fake_ip(hotfolder / "ip0")
    create_fake_ip(hotfolder / "ip1")
    create_fake_ip(hotfolder / "ip2")
    # make request for test-import
    token = client.post(
        "/import/ips",
        json={"import": {"target": {"path": str(hotfolder)}, "test": True}},
    ).json["value"]

    app.extensions["orchestra"].stop(stop_on_idle=True)
    report = client.get(f"/report?token={token}").json

    assert report["data"]["success"]
    assert len(report["data"]["IPs"]) == expected_ips
