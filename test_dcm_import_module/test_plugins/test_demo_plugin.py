"""
Test suite for the OAI-PMH-plugin.
"""

from unittest import mock

import pytest
from dcm_common import Logger

from dcm_import_module.models import IE
from dcm_import_module.plugins import IEImportResult, DemoPlugin


@pytest.mark.parametrize(
    "random",
    [True, False],
    ids=["random", "static"]
)
def test_generate_metadata_random(random):
    """Test argument `random` of method `generate_metadata` of `DemoPlugin`."""
    uuid_patch = mock.patch(
        "dcm_import_module.plugins.demo.uuid4",
        lambda: "some-uuid"
    )
    uuid_patch.start()
    class FakeDatetime:
        @staticmethod
        def now():
            return FakeDatetime()
        def isoformat(self):
            return "It is today!"
        def strftime(self, _):
            return "It is today!"
    datetime_patch = mock.patch(
        "dcm_import_module.plugins.demo.datetime",
        FakeDatetime
    )
    datetime_patch.start()
    meta1 = DemoPlugin.generate_metadata(randomize=random)
    meta2 = DemoPlugin.generate_metadata(randomize=random)

    assert (meta1 == meta2) != random

    uuid_patch.stop()
    datetime_patch.stop()


def test_generate_metadata_identifier():
    """
    Test argument `identifier` of method `generate_metadata` of `DemoPlugin`.
    """

    identifier = "test-identifier"
    meta = DemoPlugin.generate_metadata(identifier=identifier)
    assert f"<identifier>{identifier}</identifier>" in meta


@pytest.mark.parametrize(
    "number",
    [0, 1, 2],
    ids=["no-ies", "one-ie", "two-ies"]
)
def test_get(file_storage, number):
    """Test method `get` of `DemoPlugin`."""
    mock.patch(
        "dcm_import_module.plugins.demo.uuid4",
        lambda: f"some-uuid-{number}"
    ).start()

    plugin = DemoPlugin(file_storage)
    plugin_result = plugin.get(None, number=number, randomize=True)

    assert isinstance(plugin_result, IEImportResult)
    assert isinstance(plugin_result.log, Logger)

    assert len(plugin_result.ies) == number
    for ie_id, ie in plugin_result.ies.items():
        assert isinstance(ie_id, str)
        assert isinstance(ie, IE)
        assert ie.source_identifier == f"test:oai_dc:some-uuid-{number}"
        assert (ie.path / "data" / "preservation_master").is_dir()
        assert (ie.path / "meta").is_dir()
        assert (ie.path / "meta" / "source_metadata.xml").is_file()
        assert (ie.path / "data" / "preservation_master" / "payload.txt").is_file()

        assert (ie.path / "data" / "preservation_master" / "payload.txt").read_text(
            encoding="utf-8"
        ).startswith("called with")


@pytest.mark.parametrize(
    "bad_ies",
    [True, False],
    ids=["bad-ies", "good-ies"]
)
def test_get_bad_ies(file_storage, bad_ies):
    """Test method `get` of `DemoPlugin` with `bad_ies=True`."""

    plugin = DemoPlugin(file_storage)
    plugin_result = plugin.get(None, number=2, randomize=True, bad_ies=bad_ies)

    assert (
        all(ie.fetched_payload for ie in plugin_result.ies.values())
        is not bad_ies
    )
