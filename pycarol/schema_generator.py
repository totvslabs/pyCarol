import json
from datetime import datetime

class IntType(object):
    json_type = "integer"


class DoubleType(object):
    json_type = "double"


class StringType(object):
    json_type = "string"


class NullType(object):
    json_type = "string"


class BooleanType(object):
    json_type = "boolean"


class ArrayType(object):
    json_type = "object"
    items = []


class ObjectType(object):
    json_type = "object"
    properties = {}


class DateType(object):
    json_type = "date"

class Type(object):
    @classmethod
    def get_schema_type_for(cls, t):
        import pandas as pd
        import numpy as np

        """docstring for get_schema_type_for"""

        SCHEMA_TYPES = {
            type(None): NullType,
            str: StringType,
            int: IntType,
            float: DoubleType,
            bool: BooleanType,
            list: ArrayType,
            dict: ObjectType,
            np.float64: DoubleType,
            np.float32: DoubleType,
            np.float: DoubleType,
            np.int64: IntType,
            np.int32: IntType,
            np.int: IntType,
            np.bool_: BooleanType,
            datetime: DateType,
            pd.Timestamp:DateType
        }

        schema_type = SCHEMA_TYPES.get(t)

        if not schema_type:
            raise JsonSchemaTypeNotFound("There is no schema type for  %s.\n Try:\n %s" % (
            str(t), ",\n".join(["\t%s" % str(k) for k in SCHEMA_TYPES.keys()])))
        return schema_type


class JsonSchemaTypeNotFound(Exception):
    pass


class carolSchemaGenerator(object):
    def __init__(self, base_object):
        self.base_object = base_object
        self.schema_dict = None

    @classmethod
    def from_json(cls, base_json):
        base_object = json.loads(base_json)
        obj = cls(base_object)
        return obj

    def to_dict(self, mdmStagingType='stagingName', mdmFlexible=False, crosswalkname=None,
                crosswalkList=None, export_data=False):

        crosswalkname = mdmStagingType  #TODO. We can have more than 1 crosswalk, and one HAS to be named as the mdmStagingType
                                        # this is to enforce we use the same name

        assert isinstance(crosswalkList, list)


        fields = set(self.base_object.keys())
        for key_field in crosswalkList:
            if key_field not in fields:
                raise Exception('Your key field %s is not in your fields!' % (key_field))
            if type(key_field) != str:
                raise Exception('Field %s type is not string, it must be!' % key_field)

        if crosswalkname is None:
            crosswalkname = mdmStagingType

        mdmCrosswalkTemplate = {"mdmCrosswalkTemplate": {"mdmCrossreference": {crosswalkname: crosswalkList}}}

        schema_dict = {}

        base_object = self.base_object
        schema_dict["mdmStagingType"] = mdmStagingType
        schema_dict.update(mdmCrosswalkTemplate)
        schema_dict.update({"mdmStagingMapping": {"properties": {}}})
        schema_dict.update({"mdmFlexible": mdmFlexible})
        schema_dict.update({"mdmExportData": export_data})

        for prop, value in base_object.items():
            schema_dict["mdmStagingMapping"]["properties"].update({prop: _dictConstructor(base_object=value)})

        self.schema_dict = schema_dict
        return schema_dict

    def to_json(self, mdmStagingType='stagingName', mdmFlexible=False, crosswalkname=None,
                crosswalkList=None, export_data=False):
        if self.schema_dict is not None:
            json_schema = json.dumps(self.schema_dict)
        else:
            json_schema = json.dumps(self.to_dict(mdmStagingType=mdmStagingType, mdmFlexible=mdmFlexible,
                                                  crosswalkname=crosswalkname,
                                                  crosswalkList=crosswalkList, export_data=export_data))
        return json_schema


def _dictConstructor(base_object):
    schema_dict = {}
    base_object_type = type(base_object)
    schema_type = Type.get_schema_type_for(base_object_type)

    # schema_dict["required"] = True
    schema_dict["type"] = schema_type.json_type
    # schema_dict["fields"]['raw'] = schema_type.json_type

    if schema_type == ObjectType and len(base_object) > 0:
        schema_dict["properties"] = {}

        for prop, value in base_object.items():
            schema_dict["properties"][prop] = _dictConstructor(base_object=value)

    elif schema_type == ArrayType and len(base_object) > 0:
        first_item = base_object[0]
        first_item_type = type(first_item)
        same_type = all((type(item) == first_item_type for item in base_object))

        if same_type:

            first_item_schema_type = Type.get_schema_type_for(first_item_type)
            schema_dict["type"] = first_item_schema_type.json_type

            if first_item_schema_type == ObjectType:
                schema_dict.update(_dictConstructor(base_object=first_item))

        else:
            schema_dict["properties"] = {}
            #schema_dict['items'] = []

            for idx, item in enumerate(base_object):
                schema_dict['properties'].append(_dictConstructor(base_object=item))

    return schema_dict
