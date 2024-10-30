"""
Test module for the `Report` data model.
"""

from pathlib import Path

from dcm_common.models import Token, Report as BaseReport
from dcm_common.models.data_model import get_model_serialization_test

from dcm_import_module.models import Report, IE, IP, ImportResult


test_report_json = get_model_serialization_test(
    Report, (
        ((), {"host": ""}),
        ((), {
            "host": "", "token": Token(), "args": {"arg": "value"},
            "data": ImportResult(
                ies={"ie1": IE(Path("path_ie1"))},
                ips={"ip1": IP(Path("path_ip1"))}
            )
        }),
        ((), {
            "host": "", "children": {
                "test": BaseReport(host="sub-report").json
            }
        }),
    )
)
