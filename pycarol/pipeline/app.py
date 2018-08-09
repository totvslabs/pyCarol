from abc import abstractmethod
from pycarol.carol_cloner import *
from pycarol.app import CarolApp
import logging


class CarolPipelineApp(CarolApp):
    def __init__(self, carol):
        super(CarolPipelineApp, self).__init__(carol)
        pass

    def run_task(self, task):
        if task is not None:
            # TODO Run accordingly with the current environment
            task.carol_builder = Cloner(self.carol)
            task.buildme(workers=3)
            return task.output().load()
        return None

    def validate_data(self):
        return self.run_task(self.build_validation_task())

    @abstractmethod
    def build_validation_task(self, task):
        pass

    def run(self, task):
        return self.run_task(self.build_task(task))

    @abstractmethod
    def build_task(self, task):
        pass
