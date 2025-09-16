"""
ImportConfig data-model definitions
"""

from typing import Optional
from dataclasses import dataclass
from pathlib import Path

from dcm_common.models import DataModel
from dcm_common.services.plugins import PluginConfig


ImportConfigIEs = PluginConfig


@dataclass
class Target(DataModel):
    """
    Target `DataModel`

    Keyword arguments:
    path -- path to target directory/file
            (if hotfolder_id relative to that hotfolder-mount point,
            otherwise relative to `FS_MOUNT_POINT`)
    hotfolder_id -- hotfolder identifier
                    (default None)
    """

    path: Path
    hotfolder_id: Optional[str] = None

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

    @DataModel.serialization_handler("hotfolder_id", "hotfolderId")
    @classmethod
    def hotfolder_id_serialization(cls, value):
        """Performs `hotfolder_id`-serialization."""
        if value is None:
            DataModel.skip()
        return value

    @DataModel.deserialization_handler("hotfolder_id", "hotfolderId")
    @classmethod
    def hotfolder_id_deserialization(cls, value):
        """Performs `hotfolder_id`-deserialization."""
        if value is None:
            DataModel.skip()
        return value


@dataclass
class ImportConfigIPs(DataModel):
    """
    Data model for the configuration of an import of IPs.

    Keyword arguments:
    target -- `Target`-object pointing to the directory from where to
              import
    batch -- whether to search for multiple IPs in the given `Target`
             (default True)
    test -- whether to run import in test-mode
            (default False)
    """

    target: Target
    batch: bool = True
    test: bool = False
