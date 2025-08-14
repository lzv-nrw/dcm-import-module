"""
Test suite for the OAI-PMH-plugin v2.
"""

import os
from unittest import mock
from time import sleep

import pytest
from oai_pmh_extractor.oaipmh_record import OAIPMHRecord
from dcm_common import LoggingContext as Context, Logger
from dcm_common.util import list_directory_content

from dcm_import_module.plugins import OAIPMHPlugin2, IEImportResult
from dcm_import_module.models import IE


def test__validate_more():
    """Test method `_validate_more` of `OAIPMH`-plugin."""

    # bad url
    valid, msg = OAIPMHPlugin2("").validate(
        {
            "transfer_url_info": [
                {
                    "path": "",
                    "regex": ""
                }
            ],
            "base_url": "",
            "metadata_prefix": ""
        }
    )
    assert not valid

    # good request
    valid, msg = OAIPMHPlugin2("").validate(
        {
            "transfer_url_info": [
                {
                    "path": "",
                    "regex": ""
                }
            ],
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
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <GetRecord>
    <record>
      <metadata>
        <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dc="http://purl.org/dc/elements/1.1/">
          <dc:identifier>{oai_identifier}</dc:identifier>
          <dc:identifier>{oai_url + oai_identifier}_1</dc:identifier>
          <dc:another-tag>{oai_url + oai_identifier}_2</dc:another-tag>
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


@pytest.fixture(name="download_file_patcher")
def _download_file_patcher():
    """Fake `PayloadCollector.download_file`."""
    def fake_download_file(*args, **kwargs):
        _filename = f"file_{kwargs['url'].split('_')[-1]}"
        (kwargs["path"] / f"{_filename}.txt").write_text(
            kwargs["url"],
            encoding="utf-8"
        )
    return mock.patch(
        "oai_pmh_extractor.payload_collector.PayloadCollector.download_file",
        fake_download_file
    )


@pytest.mark.parametrize(
    ("transfer_url_info", "expected_files", "expected_errors"),
    [
        (
            [
                {
                    "regex": f"<dc:identifier>({_oai_url()}.*)</dc:identifier>"
                }
            ],
            ["file_1"],
            []
        ),
        (
            [
                {
                    "path": "./GetRecord/record/metadata/oai_dc:dc/dc:another-tag",
                    "regex": f"({_oai_url()}.*)"
                }
            ],
            ["file_2"],
            []
        ),
        (
            [
                {
                    "regex": f"<dc:identifier>({_oai_url()}.*)</dc:identifier>"
                },
                {
                    "path": "./GetRecord/record/metadata/oai_dc:dc/dc:another-tag",
                    "regex": f"({_oai_url()}.*)"
                }
            ],
            ["file_1", "file_2"],
            []
        ),
        (
            [
                {
                    "path": "./GetRecord/record/metadata/oai_dc:dc/adc:another-tag",
                    "regex": f"({_oai_url()}.*)"
                }
            ],
            [],
            ["prefix 'adc' not found in prefix map"]
        ),
    ],
    ids=[
        "only_regex",
        "path",
        "regex_and_path",
        "path_with_non_existent_namespace",
    ],
)
def test_get_identifiers(
    file_storage, oai_identifier, oai_url, get_record_patcher,
    list_identifiers_patcher, download_file_patcher,
    transfer_url_info, expected_files, expected_errors
):
    """Test method `get` of `OAIPMH`-plugin."""

    list_identifiers_patcher.start()
    get_record_patcher.start()
    download_file_patcher.start()

    plugin_result = OAIPMHPlugin2(file_storage).get(
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
        payload_dir = ie.path / "data" / "preservation_master"
        assert sorted(expected_files) == sorted(
            [f.stem for f in list_directory_content(payload_dir)]
        )
        for file in expected_files:
            filepath = payload_dir / f"{file}.txt"
            assert filepath.is_file()
            text = filepath.read_text(encoding="utf-8")
            assert text == (
                _oai_url() + oai_identifier + "_" + file.split("_")[-1]
            )

    if expected_errors:
        assert len(plugin_result.log[Context.ERROR]) == len(expected_errors)
        for idx, error in enumerate(expected_errors):
            assert error in plugin_result.log[Context.ERROR][idx].body
    else:
        assert Context.ERROR not in plugin_result.log

    list_identifiers_patcher.stop()
    get_record_patcher.stop()
    download_file_patcher.stop()


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
    list_identifiers_patcher,
):
    """Test method `get` of `OAIPMH`-plugin for a deleted record."""

    initial_dirs = os.listdir(file_storage)  # get dirs already in file_storage

    list_identifiers_patcher.start()
    get_deleted_record_patcher.start()

    plugin_result = OAIPMHPlugin2(file_storage).get(
        None,
        transfer_url_info=[
            {
                "path": "/metadata/oai_dc:dc/dc:identifier",
                "regex": f"({_oai_url()}.*)",
            }
        ],
        base_url=oai_url,
        metadata_prefix="",
    )

    assert isinstance(plugin_result, IEImportResult)
    assert isinstance(plugin_result.log, Logger)

    assert plugin_result.ies == {}
    assert "WARNING" in plugin_result.log.json
    assert os.listdir(file_storage) == initial_dirs  # no additional dir was created

    list_identifiers_patcher.stop()
    get_deleted_record_patcher.stop()


@pytest.fixture(name="get_record_patcher_empty_tag")
def _get_record_patcher_empty_tag(oai_identifier):
    """Fake `RepositoryInterface.get_record`."""
    def fake_get_record(*args, **kwargs):
        return OAIPMHRecord(oai_identifier, metadata_raw=f"""<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
  <GetRecord>
    <record>
      <metadata>
         <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dc="http://purl.org/dc/elements/1.1/">
          <dc:identifier>{oai_identifier}</dc:identifier>
          <dc:identifier>{_oai_url() + oai_identifier}_0</dc:identifier>
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
    list_identifiers_patcher, download_file_patcher,
):
    """Test method `get` of `OAIPMH`-plugin for a record with an empty tag."""

    list_identifiers_patcher.start()
    get_record_patcher_empty_tag.start()
    download_file_patcher.start()

    plugin_result = OAIPMHPlugin2(file_storage).get(
        None,
        transfer_url_info=[
            {
                "path": (
                    "./GetRecord/record/metadata/oai_dc:dc/dc:identifier"
                ),
                "regex": f"({_oai_url()}.*)",
            },
        ],
        base_url=oai_url,
        metadata_prefix="",
    )

    assert isinstance(plugin_result, IEImportResult)
    assert isinstance(plugin_result.log, Logger)

    assert len(plugin_result.ies) == 1

    payload_dir = (
        plugin_result.ies["ie0"].path / "data" / "preservation_master"
    )
    assert len(list_directory_content(payload_dir)) == 1
    assert (payload_dir / "file_0.txt").is_file()

    list_identifiers_patcher.stop()
    get_record_patcher_empty_tag.stop()
    download_file_patcher.stop()


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

    plugin = OAIPMHPlugin2(
        file_storage, timeout=timeout_duration, max_retries=max_retries
    )

    result = plugin.get(
        None,
        transfer_url_info=[{"regex": ""}],
        base_url="http://localhost:8083/get",
        metadata_prefix=""
    )

    assert Context.ERROR in result.log
    assert len(result.log[Context.ERROR]) == 4


@pytest.mark.parametrize(
    ("max_identifiers", "expected_ies"),
    [
        (1, 1),
        (2, 2),
        (3, 3),
        (4, 3),
    ]
)
def test_get_identifiers_test_volume(
    file_storage, oai_url, get_record_patcher, max_identifiers, expected_ies
):
    """
    Test method `get` of `OAIPMH`-plugin with different `test_volume`.
    """

    with mock.patch(
        "oai_pmh_extractor.repository_interface.RepositoryInterface.list_identifiers",
        return_value=(["a", "b", "c"], None),
    ), mock.patch(
        "oai_pmh_extractor.payload_collector.PayloadCollector.download_file",
        lambda *args, **kwargs: None,
    ):
        get_record_patcher.start()

        plugin = OAIPMHPlugin2(
            file_storage, test_strategy="random", test_volume=max_identifiers
        )

        # run in test-mode
        assert len(plugin.get(
            None,
            test=True,
            transfer_url_info=[{"regex": "()"}],
            base_url=oai_url,
            metadata_prefix="",
            max_identifiers=max_identifiers,
        ).ies) == expected_ies

        # default
        assert (
            len(
                plugin.get(
                    None,
                    transfer_url_info=[{"regex": "()"}],
                    base_url=oai_url,
                    metadata_prefix="",
                    max_identifiers=max_identifiers,
                ).ies
            )
            == 3
        )

    get_record_patcher.stop()


@pytest.mark.parametrize(
    ("max_resumption_tokens", "expected_ies"),
    [
        (-2, 2),
        (-1, 2),
        (0, 2),
        (None, 2),
        (1, 0),  # raises OverflowError, logs an error and returns no IE
        (2, 2),
    ]
)
def test_get_max_resumption_tokens(
    file_storage,
    oai_url,
    get_record_patcher,
    max_resumption_tokens,
    expected_ies,
):
    """
    Test method `get` of `OAIPMH`-plugin with different
    `max_resumption_tokens`.
    """

    max_counter = 2

    counter = [0]  # non-primitive type to enable use in fake function
    def fake_list_identifiers(_resumption_token, *args, **kwargs):
        if counter[0] < max_counter:
            counter[0] = counter[0] + 1
            return [str(counter[0])], "x"
        return [], None

    with mock.patch(
        "oai_pmh_extractor.repository_interface.RepositoryInterface.list_identifiers",
        side_effect=fake_list_identifiers,
    ), mock.patch(
        "oai_pmh_extractor.payload_collector.PayloadCollector.download_file",
        lambda *args, **kwargs: None,
    ):
        get_record_patcher.start()

        plugin = OAIPMHPlugin2(
            file_storage, max_resumption_tokens=max_resumption_tokens
        )
        result = plugin.get(
            None,
            transfer_url_info=[{"regex": "()"}],
            base_url=oai_url,
            metadata_prefix="",
        )
        assert len(result.ies) == expected_ies

    if expected_ies == 0:
        assert (
            "Encountered 'OverflowError' while 'collecting identifiers'"
            in str(result.log.json)
        )

    get_record_patcher.stop()
