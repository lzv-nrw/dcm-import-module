"""
Test suite for the 'DCM Import Module' plugin-system interface.
"""

from pathlib import Path

import pytest
from dcm_common import LoggingContext as Context
from dcm_common.plugins import Signature, Argument, JSONType

from dcm_import_module.models import IE
from dcm_import_module.plugins import (
    IEImportContext,
    IEImportResult,
    IEImportPlugin,
)


@pytest.fixture(scope="module", name="test_plugin")
def create_test_plugin(file_storage):
    """Fixture with a test plugin"""

    class TestPlugin(IEImportPlugin):
        """
        Implementation of an import-plugin-interface
        for testing purposes.
        """

        _NAME = "some-plugin"
        _DISPLAY_NAME = "Some plugin"
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
            ),
            retries=Argument(
                    type_=JSONType.INTEGER,
                    required=False,
                    default=1
            )
        )

        def _retry_iteration(self, context, **kwargs):
            context.result.ies.update(
                {
                    f"ie{len(context.result.ies)}": IE(
                        path=Path(kwargs["arg1"]) / str(kwargs["arg2"])
                    )
                }
            )
            raise ValueError("Failed.")

        def _get(
            self, context: IEImportContext, /, **kwargs
        ) -> IEImportResult:
            context.result.log.merge(
                self._retry(
                    self._retry_iteration,
                    args=(context,),
                    kwargs=kwargs,
                    exceptions=ValueError,
                )[0]
            )
            return context.result

    return TestPlugin(working_dir=file_storage, max_retries=1)


def test_get_minimal(test_plugin):
    """
    Test the `get` method of an implementation of the interface for
    minimal request.
    """

    result = test_plugin.get(None, arg1="some path", arg2=2)

    assert "ie0" in result.ies
    assert str(result.ies["ie0"].path) == "some path/2"


def test_retry(test_plugin):
    """
    Test the `get` method of an implementation of the interface for
    minimal request.
    """

    result = test_plugin.get(None, arg1="some path", arg2=2, retries=2)

    assert len(result.ies) == 2
    assert Context.ERROR in result.log
    assert len(result.log[Context.ERROR]) == 2
