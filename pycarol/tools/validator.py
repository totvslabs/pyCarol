from ..query import *
import pandas as pd
from ..utils.singleton import KeySingleton
from ..tasks import Tasks


class Validator(metaclass=KeySingleton):
    def __init__(self, carol):
        self.carol = carol
        self.cached_models = dict()
        self.messages = []

    def get_model_data(self, model):
        if model in self.cached_models:
            return self.cached_models[model]
        else:
            data = Query(self.carol).all(model).go().results
            df = pd.DataFrame(data)
            self.cached_models[model] = df
            return df

    def assert_non_empty(self, model=None, field=None, threshold=0.5, data=None):
        if data is None:
            assert model is not None
            print("Validating " + model + "." + field + "...")
            data = self.get_model_data(model)

        if field not in data.columns:
            self.add_warning({'code': 'MissingField', 'data-model': model, 'field': field})
            return
        non_empty = data.apply(lambda x: x.count(), axis=0)
        total = len(data)
        ratio = non_empty[field] / total
        if ratio < threshold:
            self.add_warning({'code': 'EmptyField', 'data-model': model, 'field': field,
                              'value': ratio, 'threshold': threshold})
        else:
            self.add_info({'code': 'EmptyField', 'data-model': model, 'field': field,
                              'value': ratio, 'threshold': threshold})

    def assert_min_quantity(self, model=None, min_qty=0, data=None):
        if data is None:
            assert model is not None
            print("Validating " + model + "...")
            data = self.get_model_data(model)

        total = len(data)
        if total < min_qty:
            self.add_warning({'code': 'ModelQuantity', 'data-model': model,
                              'value': total, 'threshold': min_qty})
        else:
            self.add_info({'code': 'ModelQuantity', 'data-model': model,
                           'value': total, 'threshold': min_qty})

    def assert_custom(self, code, value, threshold, not_message=None, yes_message=None):
        if value < threshold:
            self.add_warning({'code': code, 'value': value, 'threshold': threshold, 'message': not_message})
        else:
            self.add_info({'code': code, 'value': value, 'threshold': threshold, 'message': yes_message})

    def add_info(self, info):
        info['level'] = 'INFO'
        if 'code' not in info:
            info['code'] = 'Custom'
        self.messages.append(info)

    def add_warning(self, warning):
        warning['level'] = 'WARNING'
        if 'code' not in warning:
            warning['code'] = 'Custom'
        self.messages.append(warning)

    def results(self):
        return self.messages

    def post_results(self):
        tasks = Tasks(self.carol)
        for msg in self.messages:
            if msg['level'] == 'ERROR':
                tasks.error(msg['message'])
            elif msg['level'] == 'INFO':
                tasks.info(msg['message'])
            elif msg['level'] == 'DEBUG':
                tasks.debug(msg['message'])
            elif msg['level'] == 'WARNING':
                tasks.warn(msg['message'])
