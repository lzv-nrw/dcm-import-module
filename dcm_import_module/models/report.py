"""
Specific `Report` data-model definition for the Import Module
"""

from dataclasses import dataclass, field
from typing import Optional

from dcm_common.models import JSONObject, DataModel, Report as BaseReport

from dcm_import_module.models.import_result import ImportResult


@dataclass
class Report(BaseReport):
    data: ImportResult = field(default_factory=ImportResult)
    children: Optional[dict[str, BaseReport | JSONObject]] = None

    @DataModel.serialization_handler("children")
    @classmethod
    def children_serialization(cls, value):
        """Performs `children`-serialization."""
        if value is None:
            DataModel.skip()
        return {
            c: (r.json if isinstance(r, BaseReport) else r)
            for c, r in value.items()
        }

    @DataModel.deserialization_handler("children")
    @classmethod
    def children_deserialization(cls, value):
        """Performs `children`-deserialization."""
        if value is None:
            DataModel.skip()
        return value
