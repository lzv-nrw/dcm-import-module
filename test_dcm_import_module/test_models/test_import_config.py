"""ImportConfig-data model test-module."""

from pathlib import Path

from dcm_common.models.data_model import get_model_serialization_test

from dcm_import_module.models.import_config import (
    ImportConfigIEs, Target, ImportConfigIPs
)


test_importconfigies_json = get_model_serialization_test(
    ImportConfigIEs, (
        (("plugin", {}), {}),
    )
)


test_target_json = get_model_serialization_test(
    Target, (
        ((Path("."),), {}),
        ((Path("."), "0"), {}),
    )
)


test_importconfigips_json = get_model_serialization_test(
    ImportConfigIPs, (
        ((Target(Path(".")),), {}),
        ((Target(Path(".")), False, False), {}),
    )
)
