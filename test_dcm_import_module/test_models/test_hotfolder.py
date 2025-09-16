"""Hotfolder-data model test-module."""

from pathlib import Path

from dcm_common.models.data_model import get_model_serialization_test

from dcm_import_module.models import Hotfolder


test_hotfolder_json = get_model_serialization_test(
    Hotfolder,
    (
        ((), {"id_": "0", "mount": Path("p")}),
        (
            (),
            {
                "id_": "0",
                "mount": Path("p"),
                "name": "n",
                "description": "some description",
            },
        ),
    ),
)
