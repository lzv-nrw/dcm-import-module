"""
Test module for the `dcm_import_module/handlers.py`.
"""

import pytest
from data_plumber_http.settings import Responses

from dcm_import_module.models import ImportConfigExternal, ImportConfigInternal
from dcm_import_module import handlers


@pytest.fixture(name="external_import_handler")
def _external_import_handler(testing_config):
    return handlers.get_external_import_handler(
        testing_config().supported_plugins
    )


@pytest.fixture(name="internal_import_handler")
def _internal_import_handler(testing_config):
    return handlers.get_internal_import_handler(
        testing_config().FS_MOUNT_POINT
    )


@pytest.mark.parametrize(
    ("json", "status"),
    (
        pytest_args := [
            ({"no-import": None}, 400),
            ({"import": None}, 422),
            ({"import": {}}, 400),
            ({"import": {"unknown": None}}, 400),
            ({"import": {"plugin": None}}, 422),
            ({"import": {"plugin": None, "args": None}}, 422),
            ({"import": {"unknown": None, "plugin": None, "args": None}}, 400),
            ({"import": {"plugin": "demo", "args": None}}, 422),
            (
                {"import": {"plugin": "demo", "args": {}}},
                Responses.GOOD.status,
            ),
            (
                {
                    "import": {"plugin": "demo", "args": {}},
                    "build": {"anything": None},
                    "objectValidation": {"anything": None},
                },
                Responses.GOOD.status,
            ),
        ]
    ),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))],
)
def test_external_import_handler(external_import_handler, json, status):
    """Test external_import_handler."""

    output = external_import_handler.run(json=json)

    assert output.last_status == status
    if output.last_status == Responses.GOOD.status:
        assert "import_" in output.data.value
        assert isinstance(output.data.value["import_"], ImportConfigExternal)
    else:
        print(output.last_message)


@pytest.mark.parametrize(
    ("json", "status"),
    (
        pytest_args := [
            ({"no-import": None}, 400),
            ({"import": None}, 422),
            ({"import": {}}, 400),
            ({"import": {"unknown": None}}, 400),
            ({"import": {"target": None}}, 422),
            ({"import": {"target": {"unknown": None}}}, 400),
            ({"import": {"target": {"path": None}}}, 422),
            ({"import": {"target": {"path": "does-not-exist"}}}, 404),
            ({"import": {"target": {"path": "."}}}, Responses.GOOD.status),
            (
                {
                    "import": {"target": {"path": "."}},
                    "specificationValidation": {"anything": None},
                    "objectValidation": {"anything": None},
                },
                Responses.GOOD.status,
            ),
            ({"import": {"target": {"path": "."}, "batch": None}}, 422),
            (
                {"import": {"target": {"path": "."}, "batch": False}},
                Responses.GOOD.status,
            ),
            ({"import": {"target": {"path": "."}, "test": None}}, 422),
            (
                {"import": {"target": {"path": "."}, "test": False}},
                Responses.GOOD.status,
            ),
        ]
    ),
    ids=[f"stage {i+1}" for i in range(len(pytest_args))],
)
def test_internal_import_handler(internal_import_handler, json, status):
    """Test internal_import_handler."""

    output = internal_import_handler.run(json=json)

    assert output.last_status == status
    if output.last_status == Responses.GOOD.status:
        assert "import_" in output.data.value
        assert isinstance(output.data.value["import_"], ImportConfigInternal)
    else:
        print(output.last_message)
