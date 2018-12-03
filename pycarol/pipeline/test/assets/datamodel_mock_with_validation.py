from app.datamodel.dm_git.data_model import DataModel, Field
from app.datamodel.dm_git.validation_rule import ValidationRule, MdmUniformType

""" Test comment outside
"""
# Test comment outside


class Datamodelmock(DataModel):
    class Nestedone(Field):
        """ Test comment nested one
        """
        TYPE = Field.TYPE.NESTED

        @classmethod
        def uniform_rule(cls):
            return MdmUniformType.get_rule('currency_symbol')

        class Nestedonedeep(Field):
            TYPE = Field.TYPE.LONG

            @classmethod
            def custom_validation(cls, data):
                """ Only positive values allowed
                """
                if any(data < 0):
                    return False
                return True

    class Nestedtwo(Field):
        TYPE = Field.TYPE.DOUBLE

        @classmethod
        def validation_rule(cls):
            return ValidationRule(operation=ValidationRule.OPERATION.IS_EMPTY)
