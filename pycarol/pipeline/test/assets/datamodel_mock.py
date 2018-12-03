from ...data_model import DataModel, Field

""" Test comment outside
"""
# Test comment outside


class Datamodelmock(DataModel):
    class Nestedone(Field):
        """ Test comment nested one
        """
        TYPE = Field.TYPE.NESTED

        class Nestedonedeep(Field):
            TYPE = Field.TYPE.LONG

    class Nestedtwo(Field):
        TYPE = Field.TYPE.DOUBLE