"""ImportConfigExternal-data model test-module."""

from pathlib import Path

from dcm_common.models.data_model import get_model_serialization_test

from dcm_import_module.models.import_config import (
    ImportConfigExternal, Target, ImportConfigInternal
)


test_importconfigexternal_json = get_model_serialization_test(
    ImportConfigExternal, (
        (("plugin", {}), {}),
    )
)


test_importconfiginternal_json = get_model_serialization_test(
    ImportConfigInternal, (
        ((Target(Path(".")),), {}),
        ((Target(Path(".")), False, False), {}),
    )
)
