"""ImportResult-data model test-module."""

from pathlib import Path

from dcm_common.models.data_model import get_model_serialization_test

from dcm_import_module.models import IE, IP, ImportResult


test_importresult_json = get_model_serialization_test(
    ImportResult, (
        ((), {}),
        ((), {
            "success": True,
            "ies": {"ie1": IE(Path("path_ie1"))},
            "ips": {"ip1": IP(Path("path_ip1"))}
        }),
    )
)
