"""Utility definitions."""

import sys
from pathlib import Path
from json import loads, JSONDecodeError

from dcm_import_module.models import Hotfolder


def load_hotfolders_from_string(json: str) -> dict[str, Hotfolder]:
    """Loads hotfolders from the given JSON-string."""

    try:
        hotfolders_json = loads(json)
    except JSONDecodeError as exc_info:
        raise ValueError(
            f"Invalid hotfolder-configuration: {exc_info}."
        ) from exc_info

    if not isinstance(hotfolders_json, list):
        raise ValueError(
            "Invalid hotfolder-configuration: Expected list of hotfolders but "
            + f"got '{type(hotfolders_json).__name__}'."
        )

    hotfolders = {}
    for hotfolder in hotfolders_json:
        if not isinstance(hotfolder.get("id"), str):
            raise ValueError(
                f"Bad hotfolder id '{hotfolder.get('id')}' (bad type)."
            )
        if hotfolder["id"] in hotfolders:
            raise ValueError(
                f"Non-unique hotfolder id '{hotfolder['id']}'."
            )
        try:
            hotfolders[hotfolder["id"]] = Hotfolder.from_json(hotfolder)
        except (TypeError, ValueError) as exc_info:
            raise ValueError(
                f"Unable to deserialize hotfolder: {hotfolder}."
            ) from exc_info

    for hotfolder in hotfolders.values():
        if not hotfolder.mount.is_dir():
            print(
                "\033[1;33m"
                + f"WARNING: Mount point '{hotfolder.mount}' for hotfolder "
                + f"'{hotfolder.id_}' ({hotfolder.name or 'no name provided'})"
                + " is invalid."
                + "\033[0m",
                file=sys.stderr,
            )

    return hotfolders


def load_hotfolders_from_file(path: Path) -> dict[str, Hotfolder]:
    """Loads hotfolders from the given `path` (JSON-file)."""
    return load_hotfolders_from_string(path.read_text(encoding="utf-8"))
