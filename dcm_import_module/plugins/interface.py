"""
This module contains an interface for the definition of an import plugin
that can be used by the 'Import Module'-app to retrieve IEs via API,
e.g., OAI-PMH.

The following describes the general design of the plugin system and how
to add new plugins.

# General Notes

The plugin system is designed to enable linking of the dcm-software to
arbitrary source systems. This is implemented systematically by
introducing an interface `Interface` from which plugins should inherit.
Selective imports of data from a source system generally requires
specific sets of parameters. Hence, a plugin defines a call-signature
which is automatically passed along in an identify-request to the 'DCM
Import Module'. The definition of a signature uses the models `Signature`
and `Argument` defined in the `models`-module. Arguments can be
primitive or complex (nested). This signature is used to
* validate basic structure and types of input arguments in,
* add default values to
a (plugin-specific) import-request and
* generate the plugin-self description provided in a call to the app's
identify-endpoint.

An example plugin is given in `plugins/test_plugin.py`. An actual
implementation of a plugin for the OAI-protocol can be viewed in
`plugins/oai_pmh.py`.

# How to add a Plugin

A new plugin can inherit most requirements directly from the `Interface`
metaclass. Below are the requirements imposed by the plugin-system.

## Required definitions
* `_NAME` plugin name identifier
* `_DESCRIPTION` brief self description of plugin properties/use cases
* `_DEPENDENCIES` list of dependencies on other packages; this is used
  to provide information on dependency versions in an identify-request
* `_SIGNATURE` argument signature; used for
  * validation of input args
  * completion of input args by defaults
  * generation of info regarding input args
* `get` business logic of the plugin; requires signature
  `def get(self, **kwargs) -> PluginResult: ...`

  Note that the return value should be a `PluginResult` object.
* plugins should implement a retry-mechanism based on the constructor-
  arguments; see also `_retry`

## Useful properties
* `_get_ie_output` generates an output path inside the plugin's
  `_working_dir`; a filesystem lock is made in the moment of directory
  creation
* `_retry`: automatically impose retry-mechanism (according to plugin-
  settings) to callable; returns a log and result (if available)
* `set_progress`: a call to this function will update a progress-object
  associated with this plugin (if the plugin-instance has been
  pre-configured with `register_progress_target`, otherwise nothing is
  done); if for the progress-object something different than
  `dcm_common.models.report.Progress` is expected, the plugin's
  documentation should clearly state the expected format.

## Optional definitions
* `_validate_more` perform additional validation on input args like
  * string format,
  * value ranges,
  * ...

  This function is called with the input args as dictionary (pre-
  completion). The return value is required to be a tuple of a boolean
  (validity) and a string (message in case of error)
* `__init__` if the constructor of a plugin is changed, note that the
  `_working_dir`-`Path` should be set relative to the shared file
  system's mount point

# Register as Supported Plugin
In order for a plugin to be available for an import-job, it has to be
registered in the `Config` in `config.py`. More specifically, the
dictionary `SUPPORTED_PLUGINS` needs to have a key (identifier used in
request to select plugin; must not conflict with existing identifiers)
and an instance of the plugin. The default constructor expects a working
directory where IEs are to be stored.
"""

from typing import Optional, Any, Callable, Mapping
import abc
from dataclasses import dataclass, asdict
from importlib.metadata import version
from pathlib import Path

from dcm_common import LoggingContext as Context, Logger
from dcm_common.util import get_output_path

from dcm_import_module.models import Signature, JSONArgument, PluginResult


@dataclass
class _Package:
    """
    Class to represent a package.

    Required attributes:
    name -- name of the package
    version -- version of the package
    """
    name: str
    version: str


@dataclass
class _DependenciesVersion:
    """Class to represent a list of `_Package` objects"""
    _dependencies: list[_Package]

    @property
    def json(self) -> list[dict[str, str]]:
        """Return object as list of dictionaries"""
        return [asdict(_package) for _package in self._dependencies]


# this can be used as a decorator for defining class properties
# see https://stackoverflow.com/questions/1697501/staticmethod-with-property
# for reference
class classproperty(property):
    def __get__(self, cls, owner):
        return classmethod(self.fget).__get__(None, owner)()


class Interface(metaclass=abc.ABCMeta):
    """
    This module contains an interface for the definition of an import plugin
    that can be used by the 'Import Module'-app to retrieve IEs via API,
    e.g., OAI-PMH.

    The constructor of a plugin requires the `working_dir`-argument. It
    optionally accepts a `timeout`-argument for source systems.

    Requirements for qualification as Plugin:
    _NAME -- private property (string); identifier
             for an implementation of this interface
    _DESCRIPTION -- private property (string); short description
                    for an implementation of this interface
    _DEPENDENCIES -- private property (list[str]); list of dependencies
                     for an implementation of this interface
    _SIGNATURE -- private property (type: `Signature`);
                  signature providing exhaustive information
                  regarding the import function's arguments
                  for an implementation of this interface
                  (use `Argument` objects;
                  required attributes: "name", "type";
                  optional attribute: "default")
    get -- business logic of plugin

    Properties:
    name -- public get-method for _NAME
    description -- public get-method for _DESCRIPTION
    signature -- public get-method for _SIGNATURE
    dependencies_version -- returns a list of the versions
                                of dependencies of the `get` method
    Methods:
    get -- retrieve the IEs via API
    validate -- validate kwargs using _SIGNATURE
    """

    # setup requirements for an object to be regarded
    # as implementing the Interface
    @classmethod
    def __subclasshook__(cls, subclass):
        return (
            hasattr(subclass, "_NAME")
            and hasattr(subclass, "_DESCRIPTION")
            and hasattr(subclass, "_SIGNATURE")
            and hasattr(subclass, "_DEPENDENCIES")
            and hasattr(subclass, "get")
            and hasattr(subclass, "name")
            and hasattr(subclass, "description")
            and hasattr(subclass, "signature")
            and hasattr(subclass, "dependencies_version")
            and hasattr(subclass, "validate")
            and callable(subclass.get)
            and callable(subclass.validate)
            or NotImplemented
        )

    # setup checks for missing implementation/definition of properties
    @property
    @abc.abstractmethod
    def _NAME(self) -> str:
        """
        Identifier for an implementation of this interface;
        Expected format: str
        """

        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define property "\
                "self._NAME"
        )

    @property
    @abc.abstractmethod
    def _DESCRIPTION(self) -> str:
        """
        Short description for an implementation of this interface;
        Expected format: str
        """

        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define property "\
                "self._DESCRIPTION"
        )

    @property
    @abc.abstractmethod
    def _SIGNATURE(self) -> Signature:
        """
        Signature providing exhaustive information
        regarding the import function's arguments
        for an implementation of this interface;
        """

        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define property "\
                "self._SIGNATURE"
        )

    @property
    @abc.abstractmethod
    def _DEPENDENCIES(self) -> list[str]:
        """
        Version of dependencies for an implementation of this interface;
        """

        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define property "\
                "self._DEPENDENCIES"
        )

    @abc.abstractmethod
    def get(self, **kwargs) -> PluginResult:
        """
        Retrieve the IEs via API for an implementation of this interface.

        Keyword arguments and return value depend on the specific
        plugin-implementation (see _SIGNATURE).
        """

        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not define method "
            + "self.get."
        )

    def __init__(
        self,
        working_dir: Path,
        timeout: Optional[float] = 30,
        max_retries: int = -1,
    ) -> None:
        self._working_dir = working_dir
        self._timeout = timeout
        self._max_retries = max_retries
        self._progress: object = None
        self._push_progress = lambda: None

    # define get methods for each property
    @classproperty
    def name(cls) -> str:  # pylint: disable=no-self-argument
        """Public get-method for _NAME."""
        return cls._NAME

    @classproperty
    def description(cls) -> str:  # pylint: disable=no-self-argument
        """Public get-method for _DESCRIPTION."""
        return cls._DESCRIPTION

    @classproperty
    def signature(cls) -> JSONArgument:  # pylint: disable=no-self-argument
        """Public get-method for _SIGNATURE."""
        return cls._SIGNATURE.json

    @classproperty
    def dependencies_version(cls) -> list[dict[str, str]]:  # pylint: disable=no-self-argument
        """Public get-method for _DEPENDENCIES."""
        dependencies_version = _DependenciesVersion(
            [
                _Package(
                    name=p,
                    version=version(p)
                ) for p in cls._DEPENDENCIES
            ]
        )
        return dependencies_version.json

    def _get_ie_output(self) -> Optional[Path]:
        """
        Generates a unique identifier in `self._working_dir` and sets a
        fs-lock by creating the corresponding directory. Returns this
        directory as `Path` (`None` if not successful).
        """
        return get_output_path(self._working_dir)

    def complete(
        self,
        kwargs: JSONArgument
    ) -> JSONArgument:
        """
        Wrapper for the default value completion based on `Signature`.
        Returns a 'complete' dictionary of keyword arguments.

        Keyword arguments:
        kwargs -- dict of keyword arguments where default-values are
                  added to
        """

        return self._SIGNATURE.complete(kwargs)  # type: ignore[return-value]

    @classmethod
    def _validate_more(cls, **kwargs) -> tuple[bool, str]:
        """
        Returns tuple of boolean for validity and string-reasoning.

        Additional validation of arguments which is not captured by a
        `Signature`.
        """

        return (True, "")

    @classmethod
    def validate(
        cls,
        kwargs
    ) -> tuple[bool, str]:
        """
        Wrapper for validation of arguments against plugin `Signature`
        and additional requirements (not captured by `Signature`).
        Returns tuple of boolean for validity and string-reasoning.

        Keyword arguments:
        kwargs -- the kwargs to be validated
        """

        response = cls._SIGNATURE.validate(kwargs)
        if not response[0]:
            return response

        return cls._validate_more(**kwargs)

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
        while self._max_retries < 0 or 0 <= retry <= self._max_retries:
            try:
                result = cmd(*(args or ()), **(kwargs or {}))
                break
            except exceptions:
                log.log(
                    Context.ERROR,
                    body="Encountered timeout"
                    + (f" while '{description}'" if description else "")
                    + f". (Attempt {retry + 1}/{self._max_retries + 1})"
                )
                retry += 1
        return log, result

    def set_progress(
        self,
        **kwargs
    ) -> None:
        """
        Set the attributes of a registered object and push results (see
        `register_progress_target`).
        """
        if self._progress is None:
            return
        for name, attrib in kwargs.items():
            setattr(self._progress, name, attrib)
        self._push_progress()

    def register_progress_target(
        self, progress: object, push: Callable[[], None]
    ) -> None:
        """
        Register an object to be used for progress-updates by the
        plugin (e.g. `dcm_common.models.report.Progress`).
        """
        self._progress = progress
        self._push_progress = push
