"""
Test suite for the OAI-PMH-plugin.
"""

import os
from unittest import mock
from time import sleep

import pytest
from oai_pmh_extractor.oaipmh_record import OAIPMHRecord
from dcm_common import LoggingContext as Context, Logger

from dcm_import_module.plugins import OAIPMHPlugin, IEImportResult
from dcm_import_module.models import IE


def test__validate_more():
    """Test method `_validate_more` of `OAIPMH`-plugin."""

    # bad url
    valid, msg = OAIPMHPlugin("").validate(
        {
            "transfer_url_info": {
                "xml_path": [],
                "regex": ""
            },
            "base_url": "",
            "metadata_prefix": ""
        }
    )
    assert not valid

    # good request
    valid, msg = OAIPMHPlugin("").validate(
        {
            "transfer_url_info": {
                "xml_path": [],
                "regex": ""
            },
            "base_url": "https://lzv.nrw",
            "metadata_prefix": ""
        }
    )
    assert valid


@pytest.fixture(name="oai_identifier")
def _oai_identifier():
    return "oai:0"


def _oai_url():
    return "http://lzv.nrw/files/"


@pytest.fixture(name="oai_url")
def __oai_url():
    return _oai_url()


@pytest.fixture(name="get_record_patcher")
def _get_record_patcher(oai_identifier, oai_url):
    """Fake `RepositoryInterface.get_record`."""
    def fake_get_record(*args, **kwargs):
        return OAIPMHRecord(oai_identifier, metadata_raw=f"""<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH>
  <GetRecord>
    <record>
      <metadata>
        <oai_dc:dc>
          <dc:identifier>{oai_identifier}</dc:identifier>
          <dc:identifier>{oai_url + oai_identifier}</dc:identifier>
          <dc:another-tag>{oai_url + oai_identifier}</dc:another-tag>
        </oai_dc:dc>
      </metadata>
    </record>
  </GetRecord>
</OAI-PMH>""")
    return mock.patch(
        "oai_pmh_extractor.repository_interface.RepositoryInterface.get_record",
        fake_get_record
    )


@pytest.fixture(name="list_identifiers_patcher")
def _list_identifiers_patcher(oai_identifier):
    """Fake `RepositoryInterface.list_identifiers`."""
    def fake_list_identifiers(*args, **kwargs):
        return [oai_identifier], None
    return mock.patch(
        "oai_pmh_extractor.repository_interface.RepositoryInterface.list_identifiers",
        fake_list_identifiers
    )


@pytest.fixture(name="download_record_payload_patcher")
def _download_record_payload_patcher():
    """Fake `PayloadCollector.download_record_payload`."""
    def fake_download_record_payload(*args, **kwargs):
        (args[2] / "file.txt").write_text(
            args[1].identifier,
            encoding="utf-8"
        )
    return mock.patch(
        "oai_pmh_extractor.payload_collector.PayloadCollector.download_record_payload",
        fake_download_record_payload
    )


@pytest.mark.parametrize(
    "transfer_url_info",
    [
        {
            "xml_path": ["metadata", "oai_dc:dc", "dc:identifier"],
            "regex": f"({_oai_url()}.*)"
        },
        {
            "regex": f"<dc:identifier>({_oai_url()}.*)</dc:identifier>"
        },
    ],
    ids=["with_xml_path", "without_xml_path"]
)
def test_get_identifiers(
    file_storage, oai_identifier, oai_url, get_record_patcher,
    list_identifiers_patcher, download_record_payload_patcher,
    transfer_url_info
):
    """Test method `get` of `OAIPMH`-plugin."""

    list_identifiers_patcher.start()
    get_record_patcher.start()
    download_record_payload_patcher.start()

    plugin_result = OAIPMHPlugin(file_storage).get(
        None,
        transfer_url_info=transfer_url_info,
        base_url=oai_url,
        metadata_prefix=""
    )

    assert isinstance(plugin_result, IEImportResult)
    assert isinstance(plugin_result.log, Logger)

    for ie_id, ie in plugin_result.ies.items():
        assert isinstance(ie_id, str)
        assert isinstance(ie, IE)
        assert ie.source_identifier == oai_identifier
        assert (ie.path / "data" / "preservation_master").is_dir()
        assert (ie.path / "meta").is_dir()
        assert (ie.path / "meta" / "source_metadata.xml").is_file()
        assert (ie.path / "data" / "preservation_master" / "file.txt").is_file()

        text = (ie.path / "data" / "preservation_master" / "file.txt").read_text(
            encoding="utf-8"
        )
        assert text == oai_identifier

    list_identifiers_patcher.stop()
    get_record_patcher.stop()
    download_record_payload_patcher.stop()


@pytest.fixture(name="get_deleted_record_patcher")
def _get_deleted_record_patcher(oai_identifier):
    """Fake `RepositoryInterface.get_record`."""
    def fake_get_record(*args, **kwargs):
        return OAIPMHRecord(
            oai_identifier,
            status="deleted",
            metadata_raw=f"""<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH>
  <GetRecord>
    <record>
      <header status="deleted">
        <identifier>{oai_identifier}</identifier>
      </header>
    </record>
  </GetRecord>
</OAI-PMH>""")
    return mock.patch(
        "oai_pmh_extractor.repository_interface.RepositoryInterface.get_record",
        fake_get_record
    )


def test_get_identifiers_deleted_record(
    file_storage, oai_url, get_deleted_record_patcher,
    list_identifiers_patcher
):
    """Test method `get` of `OAIPMH`-plugin for a deleted record."""

    initial_dirs = os.listdir(file_storage) # get dirs already in file_storage

    list_identifiers_patcher.start()
    get_deleted_record_patcher.start()

    plugin_result = OAIPMHPlugin(file_storage).get(
        None,
        transfer_url_info={
            "xml_path": ["metadata", "oai_dc:dc", "dc:identifier"],
            "regex": f"({_oai_url()}.*)"
        },
        base_url=oai_url,
        metadata_prefix=""
    )

    assert isinstance(plugin_result, IEImportResult)
    assert isinstance(plugin_result.log, Logger)

    assert plugin_result.ies == {}
    assert "WARNING" in plugin_result.log.json
    assert os.listdir(file_storage) == initial_dirs # no additional dir was created

    list_identifiers_patcher.stop()
    get_deleted_record_patcher.stop()


@pytest.fixture(name="get_record_patcher_empty_tag")
def _get_record_patcher_empty_tag(oai_identifier):
    """Fake `RepositoryInterface.get_record`."""
    def fake_get_record(*args, **kwargs):
        return OAIPMHRecord(oai_identifier, metadata_raw=f"""<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH>
  <GetRecord>
    <record>
      <metadata>
        <oai_dc:dc>
          <dc:identifier>{oai_identifier}</dc:identifier>
          <dc:identifier/>
        </oai_dc:dc>
      </metadata>
    </record>
  </GetRecord>
</OAI-PMH>""")
    return mock.patch(
        "oai_pmh_extractor.repository_interface.RepositoryInterface.get_record",
        fake_get_record
    )


def test_get_identifiers_empty_tag(
    file_storage, oai_url, get_record_patcher_empty_tag,
    list_identifiers_patcher
):
    """Test method `get` of `OAIPMH`-plugin for a record with an empty tag."""

    list_identifiers_patcher.start()
    get_record_patcher_empty_tag.start()

    plugin_result = OAIPMHPlugin(file_storage).get(
        None,
        transfer_url_info={
            "xml_path": ["OAI-PMH", "GetRecord", "record", "metadata", "oai_dc:dc", "dc:identifier"],
            "regex": f"({_oai_url()}.*)"
        },
        base_url=oai_url,
        metadata_prefix=""
    )

    assert isinstance(plugin_result, IEImportResult)
    assert isinstance(plugin_result.log, Logger)

    assert len(plugin_result.ies) == 1

    list_identifiers_patcher.stop()
    get_record_patcher_empty_tag.stop()


def test_timeout_retry(file_storage, run_service):
    """Perform test for retry-behavior on external timeout."""

    # define test parameters
    timeout_duration = 0.1
    max_retries = 2

    # define service that times out
    def timeout():
        sleep(2 * timeout_duration)
    run_service(
        routes=[("/get", timeout, ["GET"])],
        port=8083
    )

    plugin = OAIPMHPlugin(
        file_storage, timeout=timeout_duration, max_retries=max_retries
    )

    result = plugin.get(
        None,
        transfer_url_info={
            "regex": ""
        },
        base_url="http://localhost:8083/get",
        metadata_prefix=""
    )

    assert Context.ERROR in result.log
    assert len(result.log[Context.ERROR]) == 4
