"""External IE-import plugin-interface."""

from typing import Optional, Any, Callable, Mapping
from dataclasses import dataclass, field
import abc
from pathlib import Path

from dcm_common import LoggingContext, Logger
from dcm_common.plugins import (
    PluginInterface,
    PluginResult,
    PluginExecutionContext,
    JSONType,
    Signature,
    Argument,
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
    `IEImportResult` and only ever extend the common `Signature` defined
    in this interface.

    Keyword arguments:
    working_dir -- output directory
    timeout -- remote system timeout duration
    max_retries -- remote system timeout duration
    max_resumption_tokens -- maximum number of processed resumption tokens;
                             only considered when positive
                             (default None leads to no restriction)
    test_strategy -- strategy for selecting identifiers during a
                     test-import (one of "first", "random")
                     (default None, uses "first")
    test_volume -- max. number of identifiers considered during a
                   test-import
                   (default 2)
    """

    _CONTEXT = "import"
    _SIGNATURE = Signature(
        test=Argument(
            JSONType.BOOLEAN,
            required=False,
            default=False,
            description="whether to run in test-mode",
            example=True,
        ),
    )
    _RESULT_TYPE = IEImportResult

    def __init__(
        self,
        working_dir: Path,
        timeout: Optional[float] = 30,
        max_retries: int = 1,
        max_resumption_tokens: Optional[int] = None,
        test_strategy: Optional[str] = None,
        test_volume: int = 2,
        **kwargs,
    ) -> None:
        super().__init__()
        self._working_dir = working_dir
        self._timeout = timeout
        self._max_retries = max_retries
        self._max_resumption_tokens = max_resumption_tokens
        if test_strategy is not None and test_strategy not in [
            "first",
            "random",
        ]:
            raise ValueError(
                f"Unknown test-strategy '{test_strategy}' in plugin "
                + f"'{self._NAME}'."
            )
        self._test_strategy = test_strategy
        if test_volume <= 0:
            raise ValueError(
                "Bad test-volume configuration in plugin '{self._NAME}': "
                + f"Value should be >= 1 (got {test_volume}) ."
            )
        self._test_volume = test_volume

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
        log = Logger(default_origin=self._DISPLAY_NAME)
        retry = 0
        while 0 <= retry <= self._max_retries:
            try:
                result = cmd(*(args or ()), **(kwargs or {}))
                break
            except exceptions as exc_info:
                log.log(
                    LoggingContext.ERROR,
                    body=f"Encountered '{type(exc_info).__name__}'"
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
