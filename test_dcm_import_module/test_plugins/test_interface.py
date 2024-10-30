"""
Test suite for the 'DCM Import Module' plugin-system interface.
"""

from pathlib import Path
from dataclasses import dataclass

import pytest
from dcm_common import Logger

from dcm_import_module.plugins.interface import Interface
from dcm_import_module.models \
    import JSONType, Signature, Argument, IE, PluginResult


@pytest.fixture(scope="module", name="test_plugin")
def create_test_plugin(file_storage):
    """Fixture with a test plugin"""

    class TestPlugin(Interface):
        """
        Implementation of an import-plugin-interface
        for testing purposes.
        """

        _NAME = "Some plugin"
        _DESCRIPTION = "Some plugin description"
        _DEPENDENCIES = ["pytest", "pip"]
        _SIGNATURE = Signature(
            arg1=Argument(
                    type_=JSONType.STRING,
                    required=True
            ),
            arg2=Argument(
                    type_=JSONType.INTEGER,
                    required=False,
                    default=1
            )
        )

        def get(self, **kwargs) -> PluginResult:
            return PluginResult(
                ies={
                    "ie1": IE(path=Path(kwargs["arg1"]) / str(kwargs["arg2"]))
                }
            )

    return TestPlugin(working_dir=file_storage)


def test_subclasshook():
    """Test method `subclasshook` of `Interface`."""

    class BadPlugin():
        _DESCRIPTION = None
        _DEPENDENCIES = None
        _SIGNATURE = None
        def get(self): return
        name = None
        description = None
        signature = None
        dependencies_version = None
        def validate(self): return

    assert not issubclass(BadPlugin, Interface)

    class GoodPlugin(BadPlugin):
        _NAME = None

    assert issubclass(GoodPlugin, Interface)


def test_name(test_plugin):
    """
    Test the `name` method
    of an implementation of the interface
    """

    assert isinstance(test_plugin.name, str)
    assert test_plugin.name == "Some plugin"


def test_description(test_plugin):
    """
    Test the `description` method
    of an implementation of the interface
    """

    assert isinstance(test_plugin.description, str)
    assert test_plugin.description == "Some plugin description"


def test_dependencies_version(test_plugin):
    """
    Test the `dependencies_version` method
    of an implementation of the interface
    """

    assert isinstance(test_plugin.dependencies_version, list)
    for y in test_plugin.dependencies_version:
        assert isinstance(y, dict)
        assert sorted(list(y.keys())) == sorted(["name", "version"])
    assert test_plugin.dependencies_version[0]["name"] == "pytest"
    assert test_plugin.dependencies_version[1]["name"] == "pip"


def test_dependencies_version_not_installed(test_plugin):
    """
    Test the `dependencies_version` method
    of an implementation of the interface
    """

    class TestPlugin2(type(test_plugin)):
        _DEPENDENCIES = ["unknown-package"]

    test_plugin2 = TestPlugin2(working_dir="")
    from importlib.metadata import PackageNotFoundError
    with pytest.raises(PackageNotFoundError):
        _ = test_plugin2.dependencies_version[0]["name"]


def test_signature(test_plugin):
    """
    Test the `signature` method
    of an implementation of the interface
    """

    assert isinstance(test_plugin.signature, dict)


def test_get_method_all_kwargs(test_plugin):
    """
    Test the `get` method
    of an implementation of the interface
    with all keyword arguments.
    """

    result = test_plugin.get(arg1="some path", arg2=2)

    assert isinstance(result, PluginResult)
    assert "ie1" in result.ies
    assert str(result.ies["ie1"].path) == "some path/2"
    assert isinstance(result.log, Logger)


def test_validate_method_unknown_argument(test_plugin):
    """
    Test the `validate` method
    of an implementation of the interface
    with an unknown argument.
    """

    result = test_plugin.validate({"arg1": "some path", "arg3": 0})
    assert not result[0]
    assert "arg3" in result[1]


def test_validate_method_missing_argument(test_plugin):
    """
    Test the `validate` method
    of an implementation of the interface
    with a missing required argument.
    """

    result = test_plugin.validate({"arg2": 0})
    assert not result[0]
    assert "arg1" in result[1]


def test_validate_method_optional(test_plugin):
    """
    Test the `validate` method
    of an implementation of the interface
    with an optional argument.
    """

    result = test_plugin.validate({"arg1": "some path"})
    assert result[0]


def test_validate_method_bad_type(test_plugin):
    """
    Test the `validate` method
    of an implementation of the interface
    with an argument of bad type.
    """

    result = test_plugin.validate({"arg1": "some path", "arg2": "2"})
    assert not result[0]
    assert "arg2" in result[1]


def test__get_ie_output_method_bad_type(test_plugin):
    """
    Test method `_get_ie_output` of the plugin `Interface`.
    """

    ie1 = test_plugin._get_ie_output()
    ie2 = test_plugin._get_ie_output()

    assert ie1 != ie2

    ie1.rmdir()
    ie2.rmdir()


def test_set_progress(test_plugin):
    """Test method `set_progress` of the plugin `Interface`."""

    @dataclass
    class Progress:
        key1: bool = False
        key2: int = 0

    expected_progress = Progress(
        key1=True,
        key2=1
    )
    # call without registering target (without error)
    test_plugin.set_progress(keyX="some-value")

    # register target and run update
    target_progress = Progress()
    test_plugin.register_progress_target(
        target_progress,
        lambda: setattr(target_progress, "key2", expected_progress.key2)
    )
    test_plugin.set_progress(key1=expected_progress.key1)

    # assert changes have been made to target_progress ...
    assert target_progress.key1 == expected_progress.key1
    # ... and push has been called
    assert target_progress.key2 == expected_progress.key2
