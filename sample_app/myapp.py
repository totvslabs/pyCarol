from pycarol.pipeline.app import *
from pycarol.pipeline.task import *
from pycarol.pipeline.tasks import MDMIngestion


class MyApp(CarolPipelineApp):
    def __init__(self, carol):
        super(MyApp, self).__init__(carol)

    def build_task(self, task):
        if task == 'train':
            pass
        elif task == 'predict':
            return PredictionOutput()

    def build_validation_task(self):
        return ValidateData()


@inherit_list(
    (MDMIngestion, dict(data_model='ordermanufacturing'))
)
class PredictionOutput(Task):
    set_target = Task.dummy_target

    def easy_run(self, inputs):
        om = inputs[0]
        # ...
        return None


@inherit_list(
    (MDMIngestion, dict(data_model='ordermanufacturing'))
)
class ValidateData(Task):
    set_target = Task.disk_target

    def easy_run(self, inputs):
        om = inputs[0]

        validator = self.build_validator()
        validator.assert_non_empty(data=om, field='abc', threshold=0.8)
        validator.assert_non_empty(data=om, field='customercode', threshold=0.8)
        validator.assert_min_quantity(data=om, min_qty=100000)

        # Custom validation
        large_ratio = (om['order4orderitemamount'].astype(float) > 5000.0).sum() / len(om)
        validator.assert_custom('LargeOrdersRatio', large_ratio, 0.2, not_message='Not enough large orders')

        # Custom warning
        validator.add_warning({'description': 'Missing XYZ data'})
        return validator.results()
