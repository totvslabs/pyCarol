""" Synchronize Data Model information on Carol

* Validate on code, what the code needs from Carol mappings

This module aims to maintain a synchronization with Carol of data model characteristics such as fields, types and
validation rules, as much as provide functionalities to organize data validation.

It works by getting from Carol all information from a data model and appending to it information from code. Some of the
validation rules, however, are not yet implemented on Carol. That's why we separate what should be updated on the MDM
and what should be only stored on code.


# TODO: Validation with external keys
# TODO: Study SQL Alchemy

"""
from ..data_models.data_model_build import DataModelBuild
import inspect

from .field import Field


class DataModel:
    _carol_name = None

    @classmethod
    def get_name(cls):
        """ Class name must be equal to field name

            E.g. Data Model with name 'dmname' should have a class named like:
                class DmName(DataModel):
                    ...
        """
        if cls._carol_name is None:
            return cls.__name__.lower()
        else:
            return cls._carol_name

    @classmethod
    def has_nested(cls):
        # TODO
        for field in cls.get_fields().values():
            if len(field.get_fields()) > 0:
                return True
        return False

    @classmethod
    def add_field(cls, field):
        """ Add Field to DataModel

        :param field: Field
        :return:
        """
        setattr(cls, field.__name__, field)

    @classmethod
    def from_carol(cls, carol):
        """ Get data from Carol to DataModel format

        :param carol_dm_dict: dictionary with this data model data from Carol

        :return: Data Model class with information from dm_dict
        """
        dm = DataModelBuild(carol).get_dm(cls.get_name())

        dm_name = dm['mdmName']

        # Create new DataModel object
        dm_class = type(dm_name.capitalize(), (DataModel,), {})

        # TODO: Get validations

        # TODO: Separate validations from fields

        # Transform all fields into classes, and all nested fields into subclasses (Dynamically)
        for field in dm['mdmFields']:
            validation = None
            f = Field.from_carol(field, validation)
            field_name = field['mdmName'].capitalize()
            setattr(dm_class, field_name, f)

        return dm_class

    @staticmethod
    def filter_metadata(dm_dict):
        """ Remove unused data from data model dict

        :param dm_dict: Dictionary with data model data from carol
        :return: Dictionary with filtered data model
        """

        data = {'mdmName': dm_dict['mdmDate'],
                'mdmFields': []}
        for field in dm_dict['mdmFields']:
            data['mdmFields'].update(Field.filter_metadata(field))
        return data

    @classmethod
    def _add_metadata(cls, carol, dm_dict, force_create=False, ignore_field_error=False):
        """ Add to class definition other information from those fields from Carol tenant

        :return:
        """
        # TODO

        fields_list = dm_dict.pop('mdmFields')
        data_model_name = dm_dict['mdmName']
        dm_carol = DataModelBuild(carol).get_dm(data_model_name)  # Get data from Carol
        dm_carol_fields = dm_carol.pop('mdmFields')

        # Merge
        dm_carol.update(dm_dict)
        dm_carol['mdmFields'] = []
        for field in fields_list:
            pos = cls._get_field_pos(field['mdmName'], dm_carol_fields)
            if pos == -1:
                if force_create:
                    new_field = Field.add_to_carol(carol, field)
                    # TODO Add to dm_carol_fields
                else:
                    if not ignore_field_error:
                        raise ValueError(f"Field {field['mdmName']} not found on Carol MDM")
            else:
                dm_carol_fields[pos].update(field)
                dm_carol['mdmFields'].append(dm_carol_fields[pos])

        return dm_carol

    @classmethod
    def get_validation_rules(cls):
        validations = {}
        for att_name in dir(cls):
            if att_name in ['validation_rule', 'uniform_rule', 'custom_validation']:
                validations.update({att_name: getattr(cls, att_name)})
        return validations

    @classmethod
    def get_fields(cls):
        """ Get Fields from this class

        :return: Dict of fields
        """
        fields = {}
        for att_name, att_val in cls.__dict__.items():
            if inspect.isclass(att_val):
                if issubclass(att_val, Field):
                    fields.update({att_name: att_val})
        return fields

    @classmethod
    def _get_field_pos(cls, field_name, fields_list):
        """ Get specific field's position from fields list

        :param field_name: Name of the desired field
        :param fields_list: Fields list
        :return: Field's position on list / -1 if not found
        """
        for pos in range(len(fields_list)):
            if field_name == fields_list[pos]['mdmName']:
                return pos
        return -1

    @classmethod
    def to_carol(cls, add_metadata=False, force_create=False):
        """ Set class to carol format

        :param add_metadata: Whether to include metadata from tenant or not
        :param force_create: If adds metadata, force field/data model/validation rule if not on Carol tenant

        :return: Dict with Data Model following Carol MDM format
        """
        # TODO
        carol_dm = {'mdmName': cls.get_name(),
                    'mdmFields': []}

        for att_name, att_val in cls.__dict__.items():
            if inspect.isclass(att_val):
                if issubclass(att_val, Field):
                    carol_dm['mdmFields'].append(att_val.to_carol(add_metadata=add_metadata, force_create=force_create))

        if add_metadata:
            return cls._add_metadata(carol_dm, force_create=force_create)

        return carol_dm

    @classmethod
    def create_validation_rules_from_fields(cls, carol=None, add_to_carol=False):
        """ Gather validation rules from all fields

        Since validation rules are exclusive to data models, we need a method to gather and create all to send to Carol

        :param carol: Carol object
        :param add_to_carol: Whether created rule should be added or not to carol
        :return: Success
        """
        rules = []
        dm_name = cls.get_name()
        for field in cls.get_fields().values():
            field_name = field.get_name()
            rules += [r.to_carol(dm_name, field_name, carol, add_to_carol) for r in field.get_nested_validation_rules()]

        return rules

    @classmethod
    def validate(cls, data, ignore_errors: bool=False):
        """ Validate if data follows Data Model definition

        :param data: Data Frame
        :return: Success, log
        """
        logs = {}
        success = True

        # TODO Validate data model format (fields)

        # Custom Validation
        if 'custom_validation' in cls.__dict__:
            # TODO Add msg as docstring
            custom_val_msg = 'Custom Validation - Check code.'
            custom_val_success = cls.custom_validation(data)
            success = success and custom_val_success
            logs.update({'custom_validation': {'success': custom_val_success,
                                               'msg': custom_val_msg}})

        # Run Fields validation
        logs['fields'] = {}
        for field_name, field in cls.get_fields().items():
            f_success, f_log = field.validate(data, ignore_errors=ignore_errors)
            success = success and f_success
            logs['fields'].update({field_name: f_log})

        return success, logs

    @classmethod
    def get_dtypes(cls):
        field_types = {}

        def _get_inner_fields(field, nested=None):
            field_name = field.get_name()
            if field.TYPE != Field.TYPE.NESTED:
                if nested is not None:
                    field_name = '_'.join(nested + [field_name])
                field_types.update({field_name: field.get_dtype()})
            else:
                if nested is None:
                    nested = [field_name]
                else:
                    nested.append(field_name)
                for f in field.get_fields().values():
                    _get_inner_fields(f, nested)

        for field in cls.get_fields().values():
            _get_inner_fields(field)

        return field_types

    @classmethod
    def update_version(cls, carol):
        #TODO
        pass