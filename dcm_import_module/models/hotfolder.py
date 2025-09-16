"""Hotfolder data-model definition"""

from typing import Optional
from dataclasses import dataclass
from pathlib import Path

from dcm_common.models import DataModel


@dataclass
class Hotfolder(DataModel):
    """
    Data model for a hotfolder.

    Keyword arguments:
    id_ -- hotfolder identifier
    mount -- path of hotfolder mount point
    name -- display-name for the hotfolder
    description -- description of the hotfolder
    """
    id_: str
    mount: Path
    name: Optional[str] = None
    description: Optional[str] = None

    @DataModel.serialization_handler("id_", "id")
    @classmethod
    def id__serialization_handler(cls, value):
        """Handles `id_`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("id_", "id")
    @classmethod
    def id__deserialization_handler(cls, value):
        """Handles `id_`-deserialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.serialization_handler("mount")
    @classmethod
    def mount_serialization_handler(cls, value):
        """Handles `mount`-serialization."""
        if value is None:
            DataModel.skip()
        return str(value)

    @DataModel.deserialization_handler("mount")
    @classmethod
    def mount_deserialization_handler(cls, value):
        """Handles `mount`-deserialization."""
        if value is None:
            DataModel.skip()
        return Path(value)
