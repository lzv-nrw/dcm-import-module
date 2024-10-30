"""
ImportResult data-model definition
"""

from typing import Optional
from dataclasses import dataclass

from dcm_common.models import DataModel

from dcm_import_module.models.ie import IE
from dcm_import_module.models.ip import IP


@dataclass
class ImportResult(DataModel):
    """
    Import result `DataModel`

    Keyword arguments:
    success -- overall success of an import job
    ies -- generated IEs by identifier (see IP.IEIdentifier)
           (default None)
    ips -- generated IPs by identifier (see IE.IPIdentifier)
           (default None)
    """

    success: Optional[bool] = None
    ies: Optional[dict[str, IE]] = None
    ips: Optional[dict[str, IP]] = None

    @DataModel.serialization_handler("ies", "IEs")
    @classmethod
    def ies_serialization(cls, value):
        """Performs `ies`-serialization."""
        if value is None:
            DataModel.skip()
        return {id_: ie.json for id_, ie in value.items()}

    @DataModel.deserialization_handler("ies", "IEs")
    @classmethod
    def ies_deserialization(cls, value):
        """Performs `ies`-deserialization."""
        if value is None:
            DataModel.skip()
        return {id_: IE.from_json(ie) for id_, ie in value.items()}

    @DataModel.serialization_handler("ips", "IPs")
    @classmethod
    def ips_serialization(cls, value):
        """Performs `ips`-serialization."""
        if value is None:
            DataModel.skip()
        return {id_: ip.json for id_, ip in value.items()}

    @DataModel.deserialization_handler("ips", "IPs")
    @classmethod
    def ips_deserialization(cls, value):
        """Performs `ips`-deserialization."""
        if value is None:
            DataModel.skip()
        return {id_: IP.from_json(ip) for id_, ip in value.items()}
