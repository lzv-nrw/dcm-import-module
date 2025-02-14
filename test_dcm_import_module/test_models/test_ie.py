"""
Test module for the `IE` data model.
"""

from pathlib import Path

from dcm_common.models.data_model import get_model_serialization_test

from dcm_import_module.models import IE


test_ie_json = get_model_serialization_test(
    IE, (
        ((Path("ie_path"),), {}),
        ((Path("ie_path"), "source-id", "ip-id", False), {}),
    )
)
