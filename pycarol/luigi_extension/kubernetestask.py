import logging
import time
import uuid
from datetime import datetime
import os

import luigi
from .dockertask import EasyDockerTask, Task

logger = logging.getLogger('luigi-interface')

from pykube.config import KubeConfig
from pykube.http import HTTPClient
from pykube.objects import Job, Pod



#TODO we can use or remove this.
class kubernetes(luigi.Config):
    auth_method = luigi.Parameter(
        default="service-account",
        description="Authorization method to access the cluster")
    kubeconfig_path = luigi.Parameter(
        default="~/.kube/config",
        description="Path to kubeconfig file for cluster authentication")
    max_retrials = luigi.IntParameter(
        default=0,
        description="Max retrials in event of job failure")


class EasyKubernetesTask(EasyDockerTask):

    """
    Need to define the following in each task:
    easy_run(self,inputs)
    image
    job_namespace
    """
    image_pull_policy = 'Never' #TODO Think if it is the best place to use this variable.
                                # possible values "IfNotPresent" or "Never" or "Always"
                                # production should be "IfNotPresent"
                                #local should be "Never"
    __POLL_TIME = 5  # see __track_job
    _kubernetes_config = None  # Needs to be loaded at runtime

    def _init_kubernetes(self):
        self.__logger = logger
        self.__logger.debug("Kubernetes auth method: " + self.auth_method)
        if self.auth_method == "kubeconfig":
            self.__kube_api = HTTPClient(KubeConfig.from_file(self.kubeconfig_path))
        elif self.auth_method == "service-account":
            self.__kube_api = HTTPClient(KubeConfig.from_service_account())
        else:
            raise ValueError("Illegal auth_method")
        self.job_uuid = str(uuid.uuid4().hex)
        now = datetime.utcnow()
        self.uu_name = "%s-%s-%s" % (self.name, now.strftime('%Y%m%d%H%M%S'), self.job_uuid[:16])

    @property
    def auth_method(self):
        """
        This can be set to ``kubeconfig`` or ``service-account``.
        It defaults to ``kubeconfig``.

        For more details, please refer to:

        - kubeconfig: http://kubernetes.io/docs/user-guide/kubeconfig-file
        - service-account: http://kubernetes.io/docs/user-guide/service-accounts
        """
        return "service-account"

    @property
    def kubeconfig_path(self):
        """
        Path to kubeconfig file used for cluster authentication.
        It defaults to "~/.kube/config", which is the default location
        when using minikube (http://kubernetes.io/docs/getting-started-guides/minikube).
        When auth_method is ``service-account`` this property is ignored.

        **WARNING**: For Python versions < 3.5 kubeconfig must point to a Kubernetes API
        hostname, and NOT to an IP address.

        For more details, please refer to:
        http://kubernetes.io/docs/user-guide/kubeconfig-file
        """
        return self.kubernetes_config.kubeconfig_path

    @property
    def name(self):
        """
        A name for this job. This task will automatically append a UUID to the
        name before to submit to Kubernetes.
        """
        #Name cannot have '.', also have to be less de 63 characters
        #TODO: Is this the best way? this will be the name of the task+package it is in.

        return '-'.join(self.__class__.__name__.lower().split('.'))
        #return '-'.join(self.get_task_family().lower().split('.')[-2:])

    @property
    def labels(self):
        """
        Return custom labels for kubernetes job.
        example::
        ``{"run_dt": datetime.date.today().strftime('%F')}``
        """
        return {}

    @property
    def spec_schema(self):
        return {
            "containers": [{
                "name": self.name,
                "image": self.image,
                "command": self.command,
                "imagePullPolicy": self.image_pull_policy,
            }],
        }

    @property
    def max_retrials(self):
        """
        Maximum number of retrials in case of failure.
        """
        return self.kubernetes_config.max_retrials

    @property
    def backoff_limit(self):
        """
        Maximum number of retries before considering the job as failed.
        See: https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/#pod-backoff-failure-policy
        """
        return 1

    @property
    def delete_on_success(self):
        """
        Delete the Kubernetes workload if the job has ended successfully.
        """
        return True

    @property
    def print_pod_logs_on_exit(self):
        """
        Fetch and print the pod logs once the job is completed.
        """
        return True

    @property
    def active_deadline_seconds(self):
        """
        Time allowed to successfully schedule pods.
        See: https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/#job-termination-and-cleanup
        """
        return None

    @property
    def kubernetes_config(self):
        if not self._kubernetes_config:
            self._kubernetes_config = kubernetes()
        return self._kubernetes_config

    def __track_job(self):
        """Poll job status while active"""
        while not self.__verify_job_has_started():
            time.sleep(self.__POLL_TIME)
            self.__logger.debug("Waiting for Kubernetes job " + self.uu_name + " to start")
        self.__print_kubectl_hints()

        status = self.__get_job_status()
        while status == "RUNNING":
            self.__logger.debug("Kubernetes job " + self.uu_name + " is running")
            time.sleep(self.__POLL_TIME)
            status = self.__get_job_status()

        assert status != "FAILED", "Kubernetes job " + self.uu_name + " failed"

        # status == "SUCCEEDED"
        self.__logger.info("Kubernetes job " + self.uu_name + " succeeded")
        self.signal_complete()

    def signal_complete(self):
        """Signal job completion for scheduler and dependent tasks.

         Touching a system file is an easy way to signal completion. example::
         .. code-block:: python

         with self.output().open('w') as output_file:
             output_file.write('')
        """
        pass

    def __get_pods(self):

        self.__logger.info(f"checking __get_pods")
        pod_objs = Pod.objects(self.__kube_api) \
            .filter(namespace=self.job_namespace, selector="job-name=" + self.uu_name) \
            .response['items']
        return [Pod(self.__kube_api, p) for p in pod_objs]

    def __get_job(self):
        jobs = Job.objects(self.__kube_api) \
            .filter(namespace=self.job_namespace, selector="luigi_task_id=" + self.job_uuid) \
            .response['items']

        assert len(jobs) == 1, "Kubernetes job " + self.job_namespace +"/"+ self.uu_name + " not found" "(job_uuid= "+  self.job_uuid + ")"
        return Job(self.__kube_api, jobs[0])

    def __print_pod_logs(self):
        for pod in self.__get_pods():
            logs = pod.logs(timestamps=True).strip()
            self.__logger.info("Fetching logs from " + pod.name)
            if len(logs) > 0:
                for l in logs.split('\n'):
                    self.__logger.info(l)

    def __print_kubectl_hints(self):
        self.__logger.info("To stream Pod logs, use:")
        for pod in self.__get_pods():
            self.__logger.info("`kubectl logs -f pod/%s`" % pod.name)

    def __verify_job_has_started(self):
        """Asserts that the job has successfully started"""
        # Verify that the job started
        self.__get_job()

        # Verify that the pod started
        pods = self.__get_pods()

        assert len(pods) > 0, "No pod scheduled by " + self.uu_name
        for pod in pods:
            status = pod.obj['status']
            for cont_stats in status.get('containerStatuses', []):
                if 'terminated' in cont_stats['state']:
                    t = cont_stats['state']['terminated']
                    err_msg = "Pod %s %s (exit code %d). Logs: `kubectl logs pod/%s`" % (
                        pod.name, t['reason'], t['exitCode'], pod.name)
                    assert t['exitCode'] == 0, err_msg

                if 'waiting' in cont_stats['state']:
                    wr = cont_stats['state']['waiting']['reason']
                    assert wr == 'ContainerCreating', "Pod %s %s. Logs: `kubectl logs pod/%s`" % (
                        pod.name, wr, pod.name)

            for cond in status.get('conditions', []):
                if 'message' in cond:
                    if cond['reason'] == 'ContainersNotReady':
                        return False
                    assert cond['status'] != 'False', \
                        "[ERROR] %s - %s" % (cond['reason'], cond['message'])
        return True

    def __get_job_status(self):
        """Return the Kubernetes job status"""
        # Figure out status and return it
        job = self.__get_job()

        if "succeeded" in job.obj["status"] and job.obj["status"]["succeeded"] > 0:
            job.scale(replicas=0)
            if self.print_pod_logs_on_exit:
                self.__print_pod_logs()
            if self.delete_on_success:
                self.__delete_job_cascade(job)
            return "SUCCEEDED"

        if "failed" in job.obj["status"]:
            failed_cnt = job.obj["status"]["failed"]
            self.__logger.debug("Kubernetes job " + self.uu_name
                                + " status.failed: " + str(failed_cnt))
            if self.print_pod_logs_on_exit:
                self.__print_pod_logs()
            if failed_cnt > self.max_retrials:
                job.scale(replicas=0)  # avoid more retrials
                return "FAILED"
        return "RUNNING"

    def __delete_job_cascade(self, job):
        delete_options_cascade = {
            "kind": "DeleteOptions",
            "apiVersion": "v1",
            "propagationPolicy": "Background"
        }
        r = self.__kube_api.delete(json=delete_options_cascade, **job.api_kwargs())
        if r.status_code != 200:
            self.__kube_api.raise_for_status(r)

    def run(self):
        if self.runlocal:
            Task.run(self)
        else:
            # KubernetesJobTask.run()
            self._init_kubernetes()
            # Render job
            job_json = self._create_job_json()
            job_json = self._add_env_variables(job_json)

            if self.active_deadline_seconds is not None:
                job_json['spec']['activeDeadlineSeconds'] = \
                    self.active_deadline_seconds
            # Update user labels
            job_json['metadata']['labels'].update(self.labels)
            # Add default restartPolicy if not specified
            if "restartPolicy" not in self.spec_schema:
                job_json["spec"]["template"]["spec"]["restartPolicy"] = "Never"
            # Submit job
            self.__logger.info("Submitting Kubernetes Job: " + self.uu_name)
            job = Job(self.__kube_api, job_json)
            job.create()
            # Track the Job (wait while active)
            self.__logger.info("Start tracking Kubernetes Job: " + self.uu_name)
            self.__track_job()

    def _create_job_json(self):
        job_json = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": self.uu_name,
                "labels": {
                    "spawned_by": "luigi",
                    "luigi_task_id": self.job_uuid
                },
                # "namespace": os.environ.get('CAROLTENANTID', '')
                "namespace": self.job_namespace
            },
            "spec": {
                "backoffLimit": self.backoff_limit,
                "template": {
                    "metadata": {
                        "name": self.uu_name
                    },
                    "spec": self.spec_schema
                }
            }
        }

        return job_json

    def _add_env_variables(self,job_json):


        env_var = dict(CAROLCONNECTORID=os.environ['CAROLCONNECTORID'],
                       CAROLAPPOAUTH=os.environ['CAROLAPPOAUTH'],
                       LONGTASKID=os.environ.get('LONGTASKID', ''),
                       CAROLTENANT=os.environ['CAROLTENANT'],
                       CAROLAPPNAME=os.environ['CAROLAPPNAME'],
                       IMAGE_NAME=os.environ['IMAGE_NAME'],
                          )
        #env_var = self.get_whole_env()
        env_var = [{'name': key, 'value': value} for key, value in env_var.items()]
        job_json['spec']['template']['spec']['containers'][0]['env'] = env_var

        return job_json
