"""

"""
import inspect
import sys
import numpy as np
from .validation_rule.validation_rule import ValidationRule
from .validation_rule.uniform_data_rule import MdmUniformType
from .validation_rule.validation_funcs import type_rules as type_rules_funcs

# TODO: Relation: if this field is related to another data model
# TODO: Type: Default value + Type custom validations


class Field:
    """ Manages Carol Field

        * validation_rule
        * uniform_rule
        * custom_validation (data)

    """
    _carol_name = None  # Field name at Carol. If None, will return Class's __name__

    _mandatory = False  # If field is mandatory or not
    _min_fill_perc = 0  # Minimum number of non null values

    def set_foreign_key(self):
        """ Set this field as a foreign key. Must return the field related to this key.
        :return:
        """
        return None

    class TYPE:
        STRING = "STRING"
        NESTED = "NESTED"
        BOOLEAN = "BOOLEAN"
        DATE = "DATE"
        LONG = "LONG"
        DOUBLE = "DOUBLE"
        BINARY = "BASE64"
        OBJECT = "OBJECT"
        GEOPOINT = "GEOPOINT"

    @classmethod
    def get_name(cls):
        """ Class name should be equal to field name. If not the case, overload this method to return field name.

            E.g. Field with name 'fieldname' should have a class named like:
                class FieldName(DataModel):
                    ...
        """
        if cls._carol_name is None:
            return cls.__name__.lower()
        else:
            return cls._carol_name

    @classmethod
    def get_fields(cls):
        """ Get Nested Fields from this class

        :return:
        """
        fields = {}
        for att_name, att_val in cls.__dict__.items():
            if inspect.isclass(att_val):
                if issubclass(att_val, Field):
                    fields.update({att_name: att_val})
        return fields

    @classmethod
    def get_validation_rules(cls):
        validations = {}
        for att_name in dir(cls):
            if att_name in ['validation_rule', 'uniform_rule', 'custom_validation']:
                validations.update({att_name: getattr(cls, att_name)})
        return validations

    @classmethod
    def add_field(cls, field):
        """ Add Field to DataModel

        :param field: Field
        :return:
        """
        setattr(cls, field.__name__, field)

    @classmethod
    def from_carol(cls, field_info, validation_info=None):
        """ Interpret data from Carol json info of a field

        :param field_info: dict with information of a field following Carol MDM Model
        :return: new Field class
        """
        # Transform all fields into classes, and all nested fields into subclasses
        field_name = field_info['mdmName'].capitalize()
        field_type = field_info['mdmMappingDataType']
        field_class = type(field_name, (Field,), {'TYPE': field_type})

        if field_type == Field.TYPE.NESTED:
            for nested_field in field_info['mdmFields']:
                nested_field_name = nested_field['mdmName'].capitalize()
                nested_field_class = Field.from_carol(nested_field)
                setattr(field_class, nested_field_name, nested_field_class)

        if validation_info is not None:
            # TODO: Add field
            pass

        return field_class

    @classmethod
    def to_carol(cls, add_metadata=False, force_create=False):
        """ Convert from Field class format to Carol dict

        :return: Carol dict
        """
        field_carol = {'mdmName': cls.get_name(),
                       'mdmMappingDataType': cls.TYPE,
                       'mdmFields': []}

        for att_name, att_val in cls.__dict__.items():
            if inspect.isclass(att_val):
                if issubclass(att_val, Field):
                    field_carol['mdmFields'].append(att_val.to_carol(add_metadata=add_metadata,
                                                                     force_create=force_create))
        return field_carol

    @classmethod
    def validate(cls, data, ignore_errors: bool=False, _nested=None, validate_nested=True):
        """ Validate if data follows validation definition

        :param data: Pandas data frame with data
        :param ignore_errors: ignore errors and continue (True) stop execution when validation error found (False)
        :param validate_nested: if True, will continue to get validation from nested fields. If False,
            logs['nested'] will return dict of fields with the format: {  field_name : (field_class, _nested) }
        :return: success (Boolean), logs (dict)
        """
        success = True
        logs = {}
        logs.update({'success': False})
        logs.update({'mandatory': cls._mandatory})
        if _nested is None:
            _nested = []
        _nested = _nested.copy()  # Python crazy way of dealing with lists
        _nested.append(cls.get_name())

        if cls.TYPE != Field.TYPE.NESTED:
            field_key = '_'.join(_nested)
            # TODO: What if fields weren't supposed to come necessarily?
            if field_key not in data.columns:
                if not ignore_errors:
                    raise ValueError(f'Could not find field {cls.get_name()} in data.')
                logs.update({'found': False})
                if cls._mandatory:
                    success = False

            else:
                logs.update({'found': True})
                data = data[field_key].dropna()
                if len(data) == 0:
                    logs.update({'non_null': False})
                    return False, logs

                # Minimum Null Validation
                if cls._min_fill_perc > 0:
                    # TODO
                    pass

                # Type Validation
                type_fun = getattr(type_rules_funcs, 'validate_' + cls.TYPE.lower())  # Get type validation function
                type_success = type_fun(data)
                logs.update({'type': {'success': type_success}})
                if not type_success:
                    if not ignore_errors:
                        raise ValueError(f'Error when validating type of field: {cls.get_name()}')
                    return type_success, logs
                success = success and type_success

                # Carol Validation Rule
                if hasattr(cls, 'validation_rule'):
                    validation_rule = cls.validation_rule()
                    validation_logs = []
                    if not isinstance(validation_rule, list):
                        validation_rule = [validation_rule]
                    for val in validation_rule:
                        if not isinstance(val, ValidationRule):
                            raise TypeError('validation_rule must return a ValidationRule object.')
                        try:
                            validation_success, val_logs = val.validate(data)
                        except TypeError as e:
                            validation_success = False
                            val_logs = str(e)
                        if not ignore_errors and not validation_success:
                            raise ValueError(f'Error when validating field [{cls.get_name()}]')
                        success = success and validation_success
                        validation_logs.append(val_logs)
                    logs.update({'validation': validation_logs})

                # Uniform_Rule Validation
                if hasattr(cls, 'uniform_rule'):
                    uniform_rule = cls.uniform_rule()
                    if not isinstance(uniform_rule, MdmUniformType):
                        raise TypeError('uniform_rule must return an MdmUniformType object.')
                    try:
                        val_success, val_logs = uniform_rule.validate(data)
                    except TypeError as e:
                        val_success = False
                        val_logs = str(e)
                    if not ignore_errors and not val_success:
                        raise ValueError(f'Error when validating uniformity of field [{cls.get_name()}]')
                    success = success and val_success
                    logs.update({'uniformity': val_logs})

                # Custom Validation
                if hasattr(cls, 'custom_validation'):
                    # TODO Add msg from function's docstring
                    custom_val_msg = 'Custom Validation - Check function code for more details.'
                    try:
                        custom_val_success = cls.custom_validation(data)
                    except TypeError as e:
                        custom_val_success = False
                        custom_val_msg = str(e)
                    success = success and custom_val_success
                    logs.update({'custom_validation': {'success': custom_val_success,
                                                       'msg': custom_val_msg}})


        else:
            # Validate nested
            logs['nested'] = {}
            logs['nested']['fields'] = {}
            for field_name, field in cls.get_fields().items():
                if validate_nested:
                    f_success, f_logs = field.validate(data, ignore_errors=ignore_errors, _nested=_nested)
                    success = success and f_success
                    logs['nested']['fields'].update({field_name: f_logs})
                else:
                    logs['nested']['fields'].update({field_name: (field, _nested)})

        logs.update({'success': success})
        return success, logs

    @classmethod
    def get_nested_validation_rules(cls):
        """ Used to transform field validation rules to data model validation rule

        :return: List with all validation rules in a dictionary format {cls: validation_rule}
        """
        validation_rule = cls.validation_rule()

        # Get nested validation rules
        rules = [{cls: validation_rule}] if validation_rule is not None else []
        for field in cls.get_fields().values():
            rules += field.get_nested_validation_rules()
        return rules

    @classmethod
    def get_dtype(cls):
        return {Field.TYPE.BOOLEAN: '?',  # boolean
                Field.TYPE.STRING: 'O',  # (Python) objects
                Field.TYPE.NESTED: 'O',  # (Python) objects
                Field.TYPE.DATE: "M",
                Field.TYPE.LONG: "i",
                Field.TYPE.DOUBLE: "f",
                Field.TYPE.BINARY: "O",
                Field.TYPE.OBJECT: "O",
                Field.TYPE.GEOPOINT: "O"
            }[cls.TYPE]

    @classmethod
    def get_parent(cls):
        """ Get class that is the Nested Field's parent

        :return: Parent's class
        """
        parents_name = cls.__qualname__.split('.')

        def rec_get_parent(i):
            if i == 0:
                return getattr(sys.modules[cls.__module__], parents_name[0])  # Nested are attr of main class
            return getattr(rec_get_parent(i-1), parents_name[i])
        return rec_get_parent(len(parents_name)-2)
