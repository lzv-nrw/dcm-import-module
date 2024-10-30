"""
IP data-model definition
"""

from typing import Optional
from pathlib import Path
from dataclasses import dataclass

from dcm_common.models import DataModel


@dataclass
class IP(DataModel):
    """
    Record class storing information on Information packages (IPs).

    Properties:
    path -- path to the IP directory
            (default None)
    valid -- validity of IP
             (default None)
    ie_identifier -- reference to IE from which this IP has been built
                     (valid only in Report.data-context)
                     (default None)
    log_id -- identifier referring to related report as listed in a
              'children'-block of a `Report`
              (default None)
    """

    path: Optional[Path] = None
    valid: Optional[bool] = None
    ie_identifier: Optional[str] = None
    log_id: Optional[str] = None

    @DataModel.serialization_handler("path")
    @classmethod
    def path_serialization(cls, value):
        """Performs `path`-serialization."""
        if value is None:
            DataModel.skip()
        return str(value)

    @DataModel.deserialization_handler("path")
    @classmethod
    def path_deserialization(cls, value):
        """Performs `path`-deserialization."""
        if value is None:
            DataModel.skip()
        return Path(value)

    @DataModel.serialization_handler("ie_identifier", "IEIdentifier")
    @classmethod
    def ie_identifier_serialization(cls, value):
        """Performs `ie_identifier`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("ie_identifier", "IEIdentifier")
    @classmethod
    def ie_identifier_deserialization(cls, value):
        """Performs `ie_identifier`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("log_id", "logId")
    @classmethod
    def log_id_serialization(cls, value):
        """Performs `log_id`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("log_id", "logId")
    @classmethod
    def log_id_deserialization(cls, value):
        """Performs `log_id`-deserialization."""
        if value is None:
            DataModel.skip()
        return value
