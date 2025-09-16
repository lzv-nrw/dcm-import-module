"""Input handlers for the 'DCM Import Module'-app."""

from typing import Mapping
from pathlib import Path

from dcm_common.services import handlers, UUID
from data_plumber_http import Property, Object, String, Url, Boolean, FileSystemObject

from dcm_import_module.plugins import IEImportPlugin
from dcm_import_module.models.import_config import Target, ImportConfigIPs


report_handler = Object(
    properties={Property("token", required=True): String()},
    accept_only=["token"],
).assemble()


def get_ies_import_handler(acceptable_plugins: Mapping[str, IEImportPlugin]):
    """
    Returns parameterized handler (based on allowed plugins)
    """
    return Object(
        properties={
            Property(
                "import", name="import_", required=True
            ): handlers.PluginType(
                acceptable_plugins,
                acceptable_context=["import"],
            ),
            Property("objectValidation", name="obj_validation"): Object(
                free_form=True
            ),
            Property("build"): Object(free_form=True),
            Property("token"): UUID(),
            Property("callbackUrl", name="callback_url"): Url(
                schemes=["http", "https"]
            ),
        },
        accept_only=[
            "import",
            "build",
            "objectValidation",
            "token",
            "callbackUrl",
        ],
    ).assemble()


ips_import_handler = Object(
    properties={
        Property("import", name="import_", required=True): Object(
            model=ImportConfigIPs,
            properties={
                Property("target", required=True): Object(
                    model=Target,
                    properties={
                        Property("path", required=True): FileSystemObject(),
                        Property("hotfolderId", "hotfolder_id"): String(),
                    },
                    accept_only=["path", "hotfolderId"],
                ),
                Property("batch", default=True): Boolean(),
                Property("test", default=False): Boolean(),
            },
            accept_only=[
                "target",
                "batch",
                "test",
            ],
        ),
        Property("specificationValidation", name="spec_validation"): Object(
            free_form=True
        ),
        Property("objectValidation", name="obj_validation"): Object(
            free_form=True
        ),
        Property("token"): UUID(),
        Property("callbackUrl", name="callback_url"): Url(
            schemes=["http", "https"]
        ),
    },
    accept_only=[
        "import",
        "specificationValidation",
        "objectValidation",
        "token",
        "callbackUrl",
    ],
).assemble()
