#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from typing import Any, cast

from thriftpy2.thrift import TType

class ThriftJSONDecoder(json.JSONDecoder):

    def __init__(self, *args, **kwargs):
        self._thrift_class = kwargs.pop('thrift_class')

        self._converters = {
            TType.STRUCT: self._to_object,
            TType.STRING: self._to_str,
            TType.I32: self._to_int,
            TType.I16: self._to_int,
            TType.BYTE: self._to_int,
            TType.I64: self._to_int,
            TType.DOUBLE: self._to_float,
            TType.BOOL: self._to_bool,
            TType.LIST: self._to_list,
            TType.SET: self._to_set,
            TType.MAP: self._to_map,
        }

        super(ThriftJSONDecoder, self).__init__(*args, **kwargs)

    def decode(self, json_str: str | dict, **kwargs):
        dct = json_str if isinstance(json_str, dict) else super(ThriftJSONDecoder, self).decode(json_str)
        return self._convert(dct, TType.STRUCT, '', self._thrift_class)

    def _convert(self, value: Any, ttype: int, field_path: str, thrift_class=None):
        if ttype not in self._converters:
            raise TypeError(f"Unrecognized thrift field type: {ttype}")

        return self._converters[ttype](value, thrift_class=thrift_class, field_path=field_path)

    def _to_object(self, value: Any, **kwargs):
        thrift_class = kwargs['thrift_class']
        thrift_spec = thrift_class.thrift_spec
        composable_obj = thrift_class()

        for field_def in thrift_spec.values():
            field_ttype, field_name, field_class, required = self._decompose_thrift_def(field_def)

            if field_name not in value and required:
                field_path_suffix = f"field {kwargs['field_path']}" if kwargs['field_path'] != '' else ''
                raise ValueError(
                    f"Field '{field_name}' is required in type {thrift_class.__name__}, but is absent in JSON {field_path_suffix}")

            if field_name not in value:
                continue
            converted_val = self._convert(value[field_name], field_ttype, f"{kwargs['field_path']}/{field_name}",
                                          field_class)
            setattr(composable_obj, field_name, converted_val)
        return composable_obj

    def _to_str(self, value: Any, **kwargs):
        return str(value)

    def _to_int(self, value: Any, **kwargs):
        return int(value)

    def _to_float(self, value: Any, **kwargs):
        return float(value)

    def _to_bool(self, value: Any, **kwargs):
        return bool(value)

    def _to_set(self, value: Any, **kwargs):
        element_ttype, _, element_def, _ = self._decompose_thrift_def(kwargs['thrift_class'])
        return {self._convert(x, element_ttype, kwargs['field_path'], element_def) for x in value}

    def _to_list(self, value: Any, **kwargs):
        element_ttype, _, element_def, _ = self._decompose_thrift_def(kwargs['thrift_class'])
        return [self._convert(x, element_ttype, kwargs['field_path'], element_def) for x in value]

    def _to_map(self, value: Any, **kwargs):
        thrift_class = kwargs['thrift_class']
        key_ttype, _, value_def, _ = self._decompose_thrift_def(thrift_class)
        val_ttype, _, val_thrift_class, _ = self._decompose_thrift_def(value_def)

        return dict([(self._convert(k, key_ttype, kwargs['field_path']),
                      self._convert(v, val_ttype, kwargs['field_path'], val_thrift_class)) for (k, v) in value.items()])

    def _decompose_thrift_def(self, field_def) -> tuple[int, str | None, Any | None, bool | None]:
        if isinstance(field_def, int):
            field_ttype = field_def
            field_name, field_class, required = None, None, None
        elif len(field_def) == 2:
            field_ttype, field_class = field_def
            field_name, required = None, None
        elif len(field_def) == 3:
            field_ttype, field_name, required = field_def
            field_class = None
        elif len(field_def) == 4:
            field_ttype, field_name, field_class, required = field_def
        else:
            raise ValueError(
                f"Received unknown length of a field definition {len(field_def)} in thrift spec {field_def}")

        return field_ttype, field_name, field_class, required


def json2thrift(json_str: str | dict, thrift_class):
    return json.loads(json_str, cls=ThriftJSONDecoder, thrift_class=thrift_class)


dict2thrift = json2thrift
