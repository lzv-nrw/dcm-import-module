"""Datamodels for the Plugins of the 'DCM Import Module'-app."""

from typing import Any, Optional, TypeAlias, MutableMapping
from copy import deepcopy


class JSONType:
    """Enum for JSON-data types."""
    STRING = "string"
    INTEGER = "integer"
    NUMBER = "number"
    BOOLEAN = "boolean"
    ARRAY = "array"
    OBJECT = "object"
    PRIMITIVE = [
        STRING, INTEGER, NUMBER, BOOLEAN
    ]
    ANY = [
        *PRIMITIVE, ARRAY, OBJECT
    ]
    MAP = {
        STRING: str,
        INTEGER: int,
        NUMBER: int | float,
        BOOLEAN: bool,
        ARRAY: list,
        OBJECT: dict,
    }
    PAM = {
        str: STRING,
        int: INTEGER,
        float: NUMBER,
        bool: BOOLEAN,
        list: ARRAY,
        dict: OBJECT
    }


JSONArgument: TypeAlias = \
    MutableMapping[str, Optional["str | int | float | bool | list | JSONArgument"]]


class Argument:
    """
    Class to represent a function argument.

    Required attributes:
    type_ -- type-name of the argument (given via `JSONType`)
    required -- if `True` this argument is mandatory in an import
                request
    description -- brief argument description
    example -- example value for argument (only `JSONType.PRIMITIVE` or
               `JSONType.ARRAY`)
    item_type -- (required only if type_ is `JSONType.ARRAY`) type of
                 array elements (only `JSONType.PRIMITIVE`)
    properties -- (required only if type_ is `JSONType.OBJECT`)
                  dictionary with `Arguments` as values

    Optional attribute:
    default -- default value of the argument
    """

    def __init__(
        self,
        type_: str,
        required: bool,
        description: Optional[str] = None,
        example: Optional[str | int | float | bool | list] = None,
        item_type: Optional[str] = None,
        properties: Optional[dict[str, "Argument"]] = None,
        default: Optional[Any] = None
    ) -> None:
        if type_ not in JSONType.ANY:
            raise ValueError(
                f"Bad type: 'type_' has to be one of '{JSONType.ANY}' not '{type_}'."
            )
        self.type_ = type_
        self.required = required
        self.description = description
        self.example = example
        # validate input: Argument is a list
        if type_ == JSONType.ARRAY:
            if item_type is None:
                raise ValueError(
                    f"Missing type for list items ('type_' is '{JSONType.ARRAY}'"
                    + " but no 'item_type' given)."
                )
            if item_type not in JSONType.PRIMITIVE:
                raise ValueError(
                    "Bad type for list items ('item_type' has to be one of "
                    + f"'{JSONType.PRIMITIVE}' not '{item_type}')."
                )
            self.item_type: Optional[str] = item_type
        else:
            self.item_type = None
        # validate input: Argument is a dict
        if type_ == JSONType.OBJECT:
            if properties is None:
                raise ValueError(
                    f"Missing child-Arguments ('type_' is '{JSONType.OBJECT}' "
                    + "but no 'properties' given)."
                )
            if default is not None:
                raise ValueError(
                    f"Illegal default value for a 'type_' of '{JSONType.OBJECT}'."
                )
            self.properties: Optional[dict[str, Argument]] = properties
        else:
            self.properties = None
        if default is not None \
                and not isinstance(default, JSONType.MAP[type_]):  # type: ignore[arg-type]
            raise ValueError(
                "Type of 'default' has to match 'type_': "
                + f"found '{type(default).__name__}' and '{JSONType.MAP[type_].__name__}'."  # type: ignore[attr-defined]
            )
        self.default = default

    @property
    def json(self) -> JSONArgument:
        """Format as json"""

        json: JSONArgument = {
            "type": self.type_,
            "required": self.required,
        }
        if self.description is not None:
            json["description"] = self.description
        if self.example is not None:
            json["example"] = self.example
        if self.default is not None:
            json["default"] = self.default
        if self.item_type is not None:
            json["itemType"] = self.item_type
        if self.properties is not None:
            json["properties"] = {}
            for name, p in self.properties.items():
                json["properties"][name] = p.json  # type: ignore[index, call-overload]
        return json

    def complete(
        self,
        arg: Optional[str | int | float | bool | list | JSONArgument]
    ) -> Optional[str | int | float | bool | list | JSONArgument]:
        """
        Returns with `arg` but has missing items replaced by their
        defaults values.

        Requires a valid call signature.
        """

        # primitive or array can have explicit defaults
        if self.type_ in JSONType.PRIMITIVE:
            # this construct is a safeguard to not overwrite existing
            # data
            if arg is None:
                return self.default
            return arg
        if self.type_ == JSONType.ARRAY:
            # make a deep copy to prevent unexpected changes in default
            # value
            if arg is None:
                return deepcopy(self.default)
            return arg

        # mypy-hint
        assert isinstance(arg, dict)
        assert isinstance(self.properties, dict)

        # objects have defaults defined only implicitly
        result: JSONArgument = {}
        for name, p in self.properties.items():
            if p.type_ == JSONType.OBJECT:
                if not p.required and name not in arg:
                    continue
                result[name] = p.complete(arg.get(name, {}))
            else:
                if not p.required and p.default is None and name not in arg:
                    continue
                result[name] = p.complete(arg.get(name, None))

        return result

    def validate(
        self,
        arg: str | int | float | bool | list | JSONArgument
    ) -> tuple[bool, str]:
        """
        Validate `arg` against `self`. Returns a tuple of `bool`
        (`True` if input is valid) and `str` (reason for result).
        """

        # validate type
        if not isinstance(arg, JSONType.MAP[self.type_]):  # type: ignore[arg-type]
            return (
                False,
                f" Argument has bad type, expected '{self.type_}'"
                + f" found '{JSONType.PAM.get(type(arg), type(arg).__name__)}'."
            )

        # validate itemType if necessary
        if self.type_ == JSONType.ARRAY:
            # mypy-hint
            assert isinstance(arg, list)
            for i in arg:
                if not isinstance(i, JSONType.MAP[self.item_type]):  # type: ignore[index, arg-type]
                    return (
                        False,
                        f" Array element has bad type, expected '{self.item_type}' "
                        + f"found '{JSONType.PAM.get(type(i), type(i).__name__)}'."
                    )

        # validate properties if necessary
        if self.type_ == JSONType.OBJECT:
            # mypy-hint
            assert isinstance(self.properties, dict)
            assert isinstance(arg, dict)

            # valdate required args are present
            for p in self.properties:
                if self.properties[p].required and p not in arg:
                    return (
                        False,
                        f" Missing required property '{p}'."
                    )
            # validate unknown args
            for p in arg:
                if p not in self.properties:
                    return (
                        False,
                        f" Unknown property property '{p}'."
                    )

            for name, p in arg.items():
                response = self.properties[name].validate(p)
                if not response[0]:
                    return (
                        False,
                        f"{name}:{response[1]}"
                    )
        return True, "Argument is valid."


class Signature(Argument):
    """
    Class to represent the full argument signature of a plugin call.

    Use as
    >>> Signature(arg1=Argument(...), arg2=Argument(...), ...)
    """

    def __init__(
        self,
        **kwargs: Argument
    ) -> None:
        super().__init__(
            type_=JSONType.OBJECT,
            required=True,
            properties=kwargs
        )
