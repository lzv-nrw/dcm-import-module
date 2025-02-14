"""
ImportConfig data-model definitions
"""

from dataclasses import dataclass
from pathlib import Path

from dcm_common.models import DataModel
from dcm_common.services.plugins import PluginConfig


ImportConfigExternal = PluginConfig


@dataclass
class Target(DataModel):
    """
    Target `DataModel`

    Keyword arguments:
    path -- path to target directory/file relative to `FS_MOUNT_POINT`
    """

    path: Path

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


@dataclass
class ImportConfigInternal(DataModel):
    """
    Data model for the configuration of an import from internal storage.

    Keyword arguments:
    target -- `Target`-object pointing to the directory from where to
              import
    batch -- whether to search for multiple IPs in the given `Target`
             (default True)
    """

    target: Target
    batch: bool = True
