"""PluginResult-data model test-module."""

from pathlib import Path

from dcm_common.models.data_model import get_model_serialization_test

from dcm_import_module.models import PluginResult, IE


test_pluginresult_json = get_model_serialization_test(
    PluginResult, (
        ((), {"ies": {"ie1": IE(Path("path_ie1"))}}),
    )
)
