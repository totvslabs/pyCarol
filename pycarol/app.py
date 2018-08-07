from abc import ABC, abstractmethod
from pycarol.validator import *
import logging


class CarolApp(ABC):
    def __init__(self, carol):
        self.carol = carol
        self.settings = None
        self.logger = logging.getLogger('pipeline-interface')
        self.logger.setLevel(logging.INFO)
        pass

    @abstractmethod
    def validate_data(self):
        pass

    @abstractmethod
    def run(self, task):
        pass

    def _load_settings(self):
        # TODO: Load app settings from carol
        self.settings = dict(
            start_year_semester="2010.1",
            end_year_semester="2018.2",
            list_size=0.02,
            select_net='rnn',
        )

    def build_validator(self):
        return Validator(self.carol)

    def get_settings(self):
        if self.settings is None:
            self._load_settings()

        return self.settings

    def get_setting(self, key):
        return self.get_settings()[key]
