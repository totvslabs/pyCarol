from ...data_models.data_model_build import DataModelBuild
from .validation_funcs import carol_validation_rules


class ValidationRule:
    """ Class to manage validation rules

    """

    class OPERATION:
        CUSTOM = 'CUSTOM_VALIDATION_FUNCTION',
        MATCHES = 'MATCHES'
        CONTAINS = 'CONTAINS'
        CONTAINS_ARRAY = 'CONTAINS_ARRAY'
        STARTS_WITH = 'STARTS_WITH'
        ENDS_WITH = 'ENDS_WITH'
        EQUALS = 'EQUALS'
        IS_EMPTY = 'IS_EMPTY'
        IS_EMAIL = 'IS_EMAIL'
        LENGTH_COMPARE = 'LENGTH_COMPARE'
        LENGTH_RANGE = 'LENGTH_RANGE'
        IS_NOT_NUMBER_ONLY = 'IS_NOT_NUMBER_ONLY'
        IS_INVALID_BRAZIL_STATE_TAX_ID = 'IS_INVALID_BRAZIL_STATE_TAX_ID'
        IS_INVALID_BRAZIL_TAX_ID = 'IS_INVALID_BRAZIL_TAX_ID'
        VALUE_COMPARE = 'VALUE_COMPARE'
        LOOKUP = 'LOOKUP'
        LOOKUP_MULTIPLE = 'LOOKUP_MULTIPLE'
        LOOKUP_FOUND = 'LOOKUP_FOUND'

    def __init__(self, operation:str =None, params:list=None, func=None):
        """

        :param operation:
        :param params:
        :param func: (Optional) Custom function
        """
        if func is None:
            if operation == ValidationRule.OPERATION.CUSTOM:
                raise AttributeError('If operation is custom, validation func must be provided.')
            func = getattr(carol_validation_rules, operation.lower())
        else:
            operation = ValidationRule.OPERATION.CUSTOM

        self.operation = operation
        self.params = params
        self.func = func

    @staticmethod
    def get_carol_operation_id(operation):
        # TODO
        pass

    def to_carol(self, dm_name, field_name, carol, add_to_carol=False):
        """ Get validation rule in Carol MDM's format

        :param carol: Carol object if want to add rule to Carol if not there
        :param add_to_carol: If rule does not exist on Carol, add it or not
        :return: Dict of the Validation Rule on Carol MDM Format
        """
        dmb = DataModelBuild(carol)
        body = {}

        if add_to_carol:
            rule_id, _ = dmb.create_rule(dm_name, field_name, self.operation, self.params)
            body.update({'mdmId': rule_id})

        template_id = dmb.get_dm(dm_name)['mdmId']
        body.update(dmb.generate_rule_body(template_id, dm_name, field_name, self.operation, self.params))
        return body

    @classmethod
    def from_carol(cls, val_rule):
        # TODO
        pass

    def validate(self, data):
        """

        :param data:
        :return: Success (boolean), logs (dict)
        """
        msg = self.operation

        # Remove non null data
        # TODO

        if self.params is not None:
            msg += ' - Params: ' + str(self.params)
            success = self.func(data, *self.params)
        else:
            success = self.func(data)

        return success, {'success': success, 'msg': msg}