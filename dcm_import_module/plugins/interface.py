"""External IE-import plugin-interface."""

from typing import Optional, Any, Callable, Mapping
from dataclasses import dataclass, field
import abc
from pathlib import Path

from dcm_common import LoggingContext as Context, Logger
from dcm_common.plugins import (
    PluginInterface,
    PluginResult,
    PluginExecutionContext,
    Signature,
)
from dcm_common.util import get_output_path

from dcm_import_module.models import IE


@dataclass
class IEImportResult(PluginResult):
    """
    Data model for the result of `IEImportPlugin`-invocations.
    """

    ies: dict[str, IE] = field(default_factory=dict)
    success: Optional[bool] = None


@dataclass
class IEImportContext(PluginExecutionContext):
    """
    Data model for the execution context of `IEImportPlugin`-
    invocations.
    """

    result: IEImportResult = field(default_factory=IEImportResult)


class IEImportPlugin(PluginInterface, metaclass=abc.ABCMeta):
    """
    External IE-import plugin-base class.

    The plugin-context is already being set here.

    An implementation's `PluginResult` should inherit from
    `IEImportResult`.

    Keyword arguments:
    working_dir -- output directory
    timeout -- remote system timeout duration
    max_retries -- remote system timeout duration
    """

    _CONTEXT = "import"
    _SIGNATURE = Signature()
    _RESULT_TYPE = IEImportResult

    def __init__(
        self,
        working_dir: Path,
        timeout: Optional[float] = 30,
        max_retries: int = 1,
        **kwargs,
    ) -> None:
        super().__init__()
        self._working_dir = working_dir
        self._timeout = timeout
        self._max_retries = max_retries

    def _get_ie_output(self) -> Optional[Path]:
        """
        Generates a unique identifier in `self._working_dir` and sets a
        fs-lock by creating the corresponding directory. Returns this
        directory as `Path` (`None` if not successful).
        """
        return get_output_path(self._working_dir)

    def _retry(
        self,
        cmd: Callable[[], Any],
        args: Optional[tuple[Any, ...]] = None,
        kwargs: Optional[Mapping[str, Any]] = None,
        description: Optional[str] = None,
        exceptions: type[Exception] | tuple[type[Exception]] = TimeoutError,
    ) -> tuple[Logger, Optional[Any]]:
        """
        Execute `cmd` up to `self._max_retries` times and return
        results if successful.

        Keyword arguments:
        cmd -- callable that should be executed
        description -- task description used in generated `Logger`
                       (default None)
        exceptions -- tuple of exceptions identified as timeout
                      (default TimeoutError)
        """

        result = None
        log = Logger(default_origin=self._NAME)
        retry = 0
        while 0 <= retry <= self._max_retries:
            try:
                result = cmd(*(args or ()), **(kwargs or {}))
                break
            except exceptions:
                log.log(
                    Context.ERROR,
                    body="Encountered timeout"
                    + (f" while '{description}'" if description else "")
                    + f". (Attempt {retry + 1}/{self._max_retries + 1})",
                )
                retry += 1
        return log, result

    @abc.abstractmethod
    def _get(self, context: IEImportContext, /, **kwargs) -> IEImportResult:
        raise NotImplementedError(
            f"Class '{self.__class__.__name__}' does not define method 'get'."
        )

    def get(  # this simply narrows down the involved types
        self, context: Optional[IEImportContext], /, **kwargs
    ) -> IEImportResult:
        return super().get(context, **kwargs)
