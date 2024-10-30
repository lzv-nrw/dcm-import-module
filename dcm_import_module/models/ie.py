"""
IE data-model definition
"""

from typing import Optional
from pathlib import Path
from dataclasses import dataclass, field

from dcm_common.models import DataModel


@dataclass
class IE(DataModel):
    """
    Record class storing information on intellectual entities (IEs).

    Properties:
    path -- path to the IE directory
    source_identifier -- source system identifier for IE
                         (default None)
    ip_identifier -- reference to IP built from this IE
                     (valid only in Report.data-context)
    fetched_payload -- `True` if the payload is present
                       (default False)
    log_id -- identifier referring to related report as listed in a
              'children'-block of a `Report`
    """

    path: Path
    source_identifier: Optional[str] = None
    ip_identifier: Optional[str] = None
    fetched_payload: bool = False
    log_id: str = field(default_factory=str)

    @DataModel.serialization_handler("path")
    @classmethod
    def path_serialization(cls, value):
        """Performs `path`-serialization."""
        return str(value)

    @DataModel.deserialization_handler("path")
    @classmethod
    def path_deserialization(cls, value):
        """Performs `path`-deserialization."""
        return Path(value)

    @DataModel.serialization_handler("source_identifier", "sourceIdentifier")
    @classmethod
    def source_identifier_serialization(cls, value):
        """Performs `source_identifier`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("source_identifier", "sourceIdentifier")
    @classmethod
    def source_identifier_deserialization(cls, value):
        """Performs `source_identifier`-deserialization."""
        return value

    @DataModel.serialization_handler("ip_identifier", "IPIdentifier")
    @classmethod
    def ip_identifier_serialization(cls, value):
        """Performs `ip_identifier`-serialization."""
        return value

    @DataModel.deserialization_handler("ip_identifier", "IPIdentifier")
    @classmethod
    def ip_identifier_deserialization(cls, value):
        """Performs `ip_identifier`-deserialization."""
        return value

    @DataModel.serialization_handler("fetched_payload", "fetchedPayload")
    @classmethod
    def fetched_payload_serialization(cls, value):
        """Performs `fetched_payload`-serialization."""
        return value

    @DataModel.deserialization_handler("fetched_payload", "fetchedPayload")
    @classmethod
    def fetched_payload_deserialization(cls, value):
        """Performs `fetched_payload`-deserialization."""
        return value

    @DataModel.serialization_handler("log_id", "logId")
    @classmethod
    def log_id_serialization(cls, value):
        """Performs `log_id`-serialization."""
        return value

    @DataModel.deserialization_handler("log_id", "logId")
    @classmethod
    def log_id_deserialization(cls, value):
        """Performs `log_id`-deserialization."""
        return value