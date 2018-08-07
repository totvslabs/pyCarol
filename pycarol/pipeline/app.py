from abc import abstractmethod
from pycarol.carol_cloner import *
from pycarol.app import CarolApp
import logging


class CarolPipelineApp(CarolApp):
    def __init__(self, carol):
        super(CarolPipelineApp, self).__init__(carol)
        pass

    def validate_data(self):
        task = self.build_validation_task()
        if task is not None:
            # TODO Run accordingly with the current environment
            task.carol_builder = Cloner(self.carol)
            task.buildme(workers=3)
            return task.output().load()
        return None

    @abstractmethod
    def build_validation_task(self, task):
        pass

    def run(self, task):
        task = self.build_task(task)
        if task is None:
            # TODO should use log instead
            print(f"Task {task} not found!")
            return None
        else:
            # TODO Run accordingly with the current environment
            task.carol_builder = Cloner(self.carol)
            return task.buildme(workers=3)

    @abstractmethod
    def build_task(self, task):
        pass
