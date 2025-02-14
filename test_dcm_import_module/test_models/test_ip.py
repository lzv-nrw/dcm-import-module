"""
Test module for the `IP` data model.
"""

from pathlib import Path

from dcm_common.models.data_model import get_model_serialization_test

from dcm_import_module.models import IP


test_ip_json = get_model_serialization_test(
    IP, (
        ((), {}),
        ((Path("ip_path"),), {}),
        ((Path("ip_path"),), {"log_id": ["id0", "id1"]}),
    )
)
