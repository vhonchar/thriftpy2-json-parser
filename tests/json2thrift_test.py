import os.path

import pytest
import thriftpy2

from src.json2thrift import json2thrift

loadedTypes = thriftpy2.load(f'{os.path.abspath(__file__)}/../example.thrift')


def test_parsing_simple_field_types():
    input_json = """
    {
      "id": 1234,
      "status": 0,
      "action": 666,
      "valid": true,
      "msgs": ["message 1", "message 2"]
    }
    """
    expected = loadedTypes.SimpleObject(1234, 0, 666, True, {"message 1", "message 2"})
    assert expected == json2thrift(input_json, loadedTypes.SimpleObject)


def test_should_omit_not_specified_fields():
    input_json = """
    {
      "not_existing_field": "this field shouldn't be in the parsed object, since it doesn't exist in the thrift schema"
    }
    """
    expected = loadedTypes.SimpleObject()
    assert expected == json2thrift(input_json, loadedTypes.SimpleObject)


def test_should_support_required_statement():
    input_json = "{}"
    with pytest.raises(ValueError) as excinfo:
        json2thrift(input_json, loadedTypes.SimpleObject)

    assert str(excinfo.value) == "Field 'id' is required in type SimpleObject, but is absent in JSON "


def test_required_validation_should_incl_field_path():
    input_json = """
    {
      "nested": {}
    }
    """
    with pytest.raises(ValueError) as excinfo:
        json2thrift(input_json, loadedTypes.ObjectWrapper)

    assert str(excinfo.value) == "Field 'id' is required in type SimpleObject, but is absent in JSON field /nested"


def test_should_omit_not_specified_fields():
    input_json = """
    {
      "id": 1234,
      "not_existing_field": "this field shouldn't be in the parsed object, since it doesn't exist in the thrift schema"
    }
    """
    expected = loadedTypes.SimpleObject(1234)
    assert expected == json2thrift(input_json, loadedTypes.SimpleObject)


def test_parse_nested_objects():
    input_json = """
    {
      "nested": {
        "id": 1234,
        "status": 0,
        "action": 666,
        "valid": true,
        "msgs": ["primary task"]
      },
      "nested_list": [
         {
           "id": 1234,
           "status": 0,
           "action": 666,
           "valid": true,
           "msgs": ["task #1"]
         },
         {
           "id": 1234,
           "status": 0,
           "action": 666,
           "valid": true,
           "msgs": ["task #2"]
         }
      ],
      "mapped_obj": {
        "key 1": {
           "id": 1234,
           "status": 0,
           "action": 666,
           "valid": true,
           "msgs": ["mapped #1"]
        },
        "key 2": {
           "id": 1234,
           "status": 0,
           "action": 666,
           "valid": true,
           "msgs": ["mapped #2"]
        }
      }
    }
    """
    expected = loadedTypes.ObjectWrapper(
        loadedTypes.SimpleObject(1234, 0, 666, True, {"primary task"}),
        [
            loadedTypes.SimpleObject(1234, 0, 666, True, {"task #1"}),
            loadedTypes.SimpleObject(1234, 0, 666, True, {"task #2"}),
        ],
        {
            'key 1': loadedTypes.SimpleObject(1234, 0, 666, True, {"mapped #1"}),
            'key 2': loadedTypes.SimpleObject(1234, 0, 666, True, {"mapped #2"}),
        },
    )

    assert expected == json2thrift(input_json, loadedTypes.ObjectWrapper)
