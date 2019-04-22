import os
from pycarol.luigi_extension.kubernetestask import EasyKubernetesTask, luigi


# -*- coding: utf-8 -*-
"""
Example Kubernetes Job Task.

to run on examples folder
# PYTHONPATH='.:..' luigi --module kubernetes_task PerlPi --local-scheduler
"""


class PerlPi(EasyKubernetesTask):

    def __init__(self, *args, **kwargs):
        super(PerlPi, self).__init__(*args, **kwargs)
        self.image = "perl"
        self.command = ["perl",  "-Mbignum=bpi", "-wle", "print bpi(1500)"]
        self.namespace = "default"

    # defining the two functions below allows for dependency checking,
    # but isn't a requirement
    # def signal_complete(self):
    #     with self.output().open('w') as output:
    #         output.write('')

    # def output(self):
    #     target = os.path.join("/tmp", "PerlPi")
    #     return luigi.LocalTarget(target)
