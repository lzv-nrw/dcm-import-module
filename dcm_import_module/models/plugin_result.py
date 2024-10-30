"""
PluginResult data-model definition
"""

from dataclasses import dataclass, field

from dcm_common.models import DataModel
from dcm_common import Logger

from dcm_import_module.models.ie import IE


@dataclass
class PluginResult(DataModel):
    """
    Plugin result `DataModel`

    Keyword arguments:
    IEs -- generated IEs by identifier
    log -- `BaseReport` object
    """

    ies: dict[str, IE] = field(default_factory=dict)
    log: Logger = field(default_factory=Logger)

    @DataModel.serialization_handler("ies", "IEs")
    @classmethod
    def ies_serialization(cls, value):
        """Performs `ies`-serialization."""
        return {id_: ie.json for id_, ie in value.items()}

    @DataModel.deserialization_handler("ies", "IEs")
    @classmethod
    def ies_deserialization(cls, value):
        """Performs `ies`-deserialization."""
        return {id_: IE.from_json(ie) for id_, ie in value.items()}
