from pycarol.app import *
from pycarol.carol import *
from pycarol.auth.PwdAuth import *
from pycarol.auth.ApiKeyAuth import *
from pycarol.query import *
from pycarol.server import *
from pycarol.storage import *
from timeit import default_timer as timer

# TODO
# Luigi Integration
# Model Storage versioning


class MyApp(CarolApp):
    def __init__(self, carol):
        super(MyApp, self).__init__(carol, luigi=True)
        pass

    def validate_data(self):
        validator = self.build_validator()
        validator.assert_non_empty('ordermanufacturing', 'abc', threshold=0.8)
        validator.assert_non_empty('ordermanufacturing', 'customercode', threshold=0.8)
        validator.assert_min_quantity('ordermanufacturing', 100000)

        # Custom validation
        df_om = validator.get_model_data('ordermanufacturing')
        large_ratio = (df_om['order4orderitemamount'].astype(float) > 5000.0).sum() / len(df_om)
        validator.assert_custom('LargeOrdersRatio', large_ratio, 0.2, not_message='Not enough large orders')

        # Custom warning
        validator.add_warning({'description': 'Missing XYZ data'})
        return validator.results()

    def requires(self):
        # ....
        pass
