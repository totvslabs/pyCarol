# -*- coding: utf-8 -*-
import random as rnd
import time
from pycarol.pipeline import Task, inherit_list
Task.is_cloud_target = False
import luigi


class Configuration(Task):
    seed = luigi.IntParameter()

    def easy_run(self, inputs):
        time.sleep()
        rnd.seed(self.seed)

        result = ','.join(
            [str(x) for x in rnd.sample(list(range(300)), rnd.randint(3, 5))])
        return result


class Data(Task):
    magic_number = luigi.IntParameter()

    def easy_run(self, inputs):
        time.sleep(1)
        return self.magic_number


@inherit_list(
    Configuration
)
class Dynamic(Task):
    seed = luigi.IntParameter(default=1)

    def easy_run(self, inputs):

        data = [int(x) for x in inputs[0].split(',')]
        #Generate requeires on run time.
        ata_dependent_deps = [Data(magic_number=x) for x in data]
        yield ata_dependent_deps
        return ata_dependent_deps



@inherit_list(
    Dynamic
)
class NextDynamic(Task):
    seed = luigi.IntParameter(default=1)

    def easy_run(self, inputs):

        #load the dependencies of the the dynamicly created tasks.
        ata_dependent_deps = [x.load() for x in inputs[0]]
        pass





if __name__ == '__main__':
    luigi.build([NextDynamic(seed=1)], local_scheduler=True)