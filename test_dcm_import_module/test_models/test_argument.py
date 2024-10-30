"""
Test module for the `Argument` data model.
"""

import pytest

from dcm_import_module.models import JSONType, Argument


@pytest.mark.parametrize(
    "arguments",
    [
        {"type_": "unknown"},
        {"type_": JSONType.STRING, "default": 0},
        {"type_": JSONType.ARRAY},
        {"type_": JSONType.ARRAY, "item_type": JSONType.OBJECT},
        {"type_": JSONType.OBJECT},
        {"type_": JSONType.OBJECT, "properties": {}, "default": {}},
    ],
    ids=[
        "bad_type",
        "bad_default_type",
        "missing_itemType",
        "bad_itemType",
        "missing_properties",
        "obj_illegal_default"
    ]
)
def test_argument_constructor(arguments):
    """Test exception-behavior for constructor of class `Argument`."""

    with pytest.raises(ValueError):
        Argument(required=False, **arguments)


@pytest.mark.parametrize(
    ("argument", "data", "expected"),
    [
        (  # primitive_ok
            Argument(JSONType.STRING, False),
            "some string",
            True
        ),
        (  # primitive_bad
            Argument(JSONType.STRING, False),
            0,
            False
        ),
        (  # array_ok
            Argument(JSONType.ARRAY, False, item_type=JSONType.STRING),
            ["strin1", "string2"],
            True
        ),
        (  # array_bad
            Argument(JSONType.ARRAY, False, item_type=JSONType.STRING),
            [0, 1],
            False
        ),
        (  # object_ok
            Argument(
                JSONType.OBJECT,
                False,
                properties={
                    "p1": Argument(JSONType.STRING, False)
                }
            ),
            {"p1": "some string"},
            True
        ),
        (  # object_bad
            Argument(
                JSONType.OBJECT,
                False,
                properties={
                    "p1": Argument(JSONType.STRING, False)
                }
            ),
            {"p1": 0},
            False
        ),
        (  # missing_required
            Argument(
                JSONType.OBJECT,
                False,
                properties={
                    "p1": Argument(JSONType.STRING, False),
                    "p2": Argument(JSONType.STRING, True)
                }
            ),
            {"p1": "some string"},
            False
        ),
        (  # unknown
            Argument(
                JSONType.OBJECT,
                False,
                properties={
                    "p1": Argument(JSONType.STRING, False),
                }
            ),
            {"p2": 0},
            False
        ),
    ],
    ids=[
        "primitive_ok",
        "primitive_bad",
        "array_ok",
        "array_bad",
        "object_ok",
        "object_bad",
        "missing_required",
        "unknown",
    ]
)
def test_argument_validation(argument, data, expected):
    """Test `Argument`-validation."""

    assert argument.validate(data)[0] == expected


@pytest.mark.parametrize(
    ("argument", "json"),
    [
        (  # primitive
            Argument(JSONType.STRING, False),
            {
                "type": JSONType.STRING,
                "required": False
            }
        ),
        (  # array
            Argument(JSONType.ARRAY, False, item_type=JSONType.STRING),
            {
                "type": JSONType.ARRAY,
                "required": False,
                "itemType": JSONType.STRING
            }
        ),
        (  # object
            Argument(
                JSONType.OBJECT,
                False,
                properties={
                    "p1": Argument(JSONType.STRING, False)
                }
            ),
            {
                "type": JSONType.OBJECT,
                "required": False,
                "properties": {
                    "p1": {
                        "type": JSONType.STRING,
                        "required": False
                    }
                }
            }
        ),
        (  # default_primitive
            Argument(JSONType.STRING, False, default="some string"),
            {
                "type": JSONType.STRING,
                "required": False,
                "default": "some string"
            }
        ),
        (  # default_array
            Argument(
                JSONType.ARRAY, False, item_type=JSONType.INTEGER, default=[0, 1]
            ),
            {
                "type": JSONType.ARRAY,
                "required": False,
                "itemType": JSONType.INTEGER,
                "default": [0, 1]
            }
        ),
    ],
    ids=[
        "primitive",
        "array",
        "object",
        "default_primitive",
        "default_array",
    ]
)
def test_argument_json(argument, json):
    """Test `Argument`'s `json` property."""

    assert argument.json == json


@pytest.mark.parametrize(
    ("arg", "in_", "out"),
    [
        (
            Argument(JSONType.INTEGER, True, default=1),
            -1,
            -1,
        ),
        (
            Argument(JSONType.INTEGER, True, default=2),
            None,
            2,
        ),
        (
            Argument(JSONType.INTEGER, False),
            None,
            None,
        ),
        (
            Argument(JSONType.INTEGER, False),
            4,
            4,
        ),
        (
            Argument(JSONType.INTEGER, False, default=5),
            None,
            5,
        ),
        (
            Argument(JSONType.OBJECT, False, properties={
                "q1": Argument(JSONType.INTEGER, True)
            }),
            None,
            None,
        ),
        (
            Argument(JSONType.OBJECT, True, properties={
                "q1": Argument(JSONType.INTEGER, False, default=71)
            }),
            {},
            {
                "q1": 71
            },
        ),
        (
            Argument(JSONType.OBJECT, False, properties={
                "q1": Argument(JSONType.INTEGER, False)
            }),
            {},
            {},
        ),
        (
            Argument(JSONType.INTEGER, True, default=1),
            0,
            0,
        )
    ]
)
def test_argument_complete(arg, in_, out):
    """Test `Argument`'s `complete`-method."""

    argument = Argument(
        JSONType.OBJECT,
        False,
        properties={"p": arg}
    )
    if in_ is None:
        in__ = {}
    else:
        in__ = {"p": in_}
    if out is None:
        out_ = {}
    else:
        out_ = {"p": out}
    assert argument.complete(in__) == out_
