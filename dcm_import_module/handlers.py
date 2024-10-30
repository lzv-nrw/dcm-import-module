"""Input handlers for the 'DCM Import Module'-app."""

from pathlib import Path

from dcm_common.services import TargetPath
from data_plumber_http import Property, Object, String, Url, Boolean

from dcm_import_module.models.import_config import (
    ImportConfigExternal, Target, ImportConfigInternal
)


report_handler = Object(
    properties={
        Property("token", required=True): String()
    },
    accept_only=["token"]
).assemble()


def get_external_import_handler(plugins: list[str]):
    """
    Returns parameterized handler (based on allowed plugins)
    """
    return Object(
        properties={
            Property("import", name="import_", required=True): Object(
                model=ImportConfigExternal,
                properties={
                    Property("plugin", required=True): String(enum=plugins),
                    Property("args", required=True): Object(free_form=True),
                },
                accept_only=[
                    "plugin", "args",
                ]
            ),
            Property("validation"): Object(free_form=True),
            Property("build"): Object(free_form=True),
            Property("callbackUrl", name="callback_url"):
                Url(schemes=["http", "https"])
        },
        accept_only=["import", "build", "validation", "callbackUrl"]
    ).assemble()


def get_internal_import_handler(cwd: Path):
    """
    Returns parameterized handler (based on cwd)
    """
    return Object(
        properties={
            Property("import", name="import_", required=True): Object(
                model=ImportConfigInternal,
                properties={
                    Property("target", required=True): Object(
                        model=Target,
                        properties={
                            Property("path", required=True):
                                TargetPath(
                                    _relative_to=cwd, cwd=cwd, is_dir=True
                                )
                        },
                        accept_only=["path"]
                    ),
                    Property("batch", default=True): Boolean()
                },
                accept_only=[
                    "target", "batch",
                ]
            ),
            Property("validation"): Object(free_form=True),
            Property("callbackUrl", name="callback_url"):
                Url(schemes=["http", "https"])
        },
        accept_only=["import", "validation", "callbackUrl"]
    ).assemble()
