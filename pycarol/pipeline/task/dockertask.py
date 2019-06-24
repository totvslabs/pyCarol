
from luigi import BoolParameter
from pycarol.pipeline.task import Task

import os

        
class EasyDockerTask(Task):
    runlocal = BoolParameter(significant=False)  # default is False. So it starts a container by default.

    def _cmd_params(self):
        l = []
        for k, v in self.to_str_params().items():
            if k == 'runlocal':
                continue
            l.append("--{}".format(k).replace("_", "-"))
            l.append(v)
        return l

    @property
    def command(self):
        cmd = ["luigi","--local-scheduler", self.task_family, "--module", self.task_module, "--runlocal",*self._cmd_params() ]
        return cmd

    def get_whole_env(self):
        return {k: v for k, v in os.environ.items()}

    def run(self):
        if self.runlocal:
            print("been there inside a container",self)

            Task.run(self)
        else:
            print("been there before calling docker stuff",self.command)

            docker_environment = self.get_whole_env()
            docker_environment["PYTHONPATH"]="." #TODO isso pode deixar direto na imagem
            print("docker environment\n",docker_environment)

            import docker
            client = docker.from_env()

            client.containers.run(
                self.image,
                self.command,
                environment= docker_environment,
                stream=True,
                # detach=True,
            )
