import logging
import os
import ssl
import time
import uuid
from datetime import datetime

import kubernetes.client
import luigi
from kubernetes import client, config, utils
from kubernetes.client.rest import ApiException

from pycarol.luigi_extension.dockertask import EasyDockerTask, Task

import urllib3
urllib3.disable_warnings()


logger = logging.getLogger('luigi-interface')

class EasyKubernetesTask(EasyDockerTask):
    """
    Need to define the following in each task:
    easy_run(self,inputs)
    """

    def __init__(self, *args, **kwargs):
        super(EasyKubernetesTask, self).__init__(*args, **kwargs)
        self.__image_pull_policy = 'Never' #TODO Think if it is the best place to use this variable.
                                    # possible values "IfNotPresent" or "Never" or "Always"
                                    # production should be "IfNotPresent"
                                    #local should be "Never"
        self.__POLL_TIME = 5  # see __track_job
        self.__kubernetes_config = None  # Needs to be loaded at runtime
        self.__kubeconfig_path = '~/.kube/config'
        self.__auth_method = "service-account"
        self.__active_deadline_seconds = None
        self.__restart_policy = "Never"
        self.__namespace = 'carol-jobs'
        self.__image = os.environ.get('IMAGE', 'busibox')
        self.__command = ''
        self.__args = []
        self.__labels = {}

    def __init_kubernetes(self):
        self.__logger = logger
        self.__logger.debug("Kubernetes auth method: " + self.auth_method)
        if self.auth_method == "kubeconfig":
            config.load_kube_config(config_file=self.kubeconfig_path)
        elif self.auth_method == "service-account":
            config.load_kube_config()
        else:
            raise ValueError("Illegal auth_method")
        self.__kubernetes_config = kubernetes.client.Configuration()
        self.__api_instance = kubernetes.client.BatchV1Api(
            kubernetes.client.ApiClient(self.__kubernetes_config)
        )

        self.job_uuid = str(uuid.uuid4().hex)
        now = datetime.utcnow()
        self.uu_name = "%s-%s-%s" % (self.name, now.strftime('%Y%m%d%H%M%S'), self.job_uuid[:16])

    @property
    def auth_method(self):
        """
        This can be set to ``kubeconfig`` or ``service-account``.
        It defaults to ``service-account``.

        For more details, please refer to:

        - kubeconfig: http://kubernetes.io/docs/user-guide/kubeconfig-file
        - service-account: http://kubernetes.io/docs/user-guide/service-accounts
        """
        return self.__auth_method

    @auth_method.setter
    def auth_method(self, method):
        self.__auth_method = method

    @property
    def restart_policy(self):
        return self.__restart_policy if self.__restart_policy is not None else 'Never'

    @restart_policy.setter
    def restart_policy(self, policy):
        self.__restart_policy = policy

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
        return self.__kubeconfig_path

    @kubeconfig_path.setter
    def kubeconfig_path(self, path):
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
        self.__kubeconfig_path = path

    @property
    def name(self):
        """
        A name for this job. This task will automatically append a UUID to the
        name before to submit to Kubernetes.
        """
        #Name cannot have '.', also have to be less de 63 characters
        #TODO: Is this the best way? this will be the name of the task+package it is in.

        return '-'.join(self.__class__.__name__.lower().split('.'))

    @property
    def image(self):
        return self.__image

    @image.setter
    def image(self, image_name):
        self.__image = image_name

    @property
    def namespace(self):
        return self.__namespace

    @namespace.setter
    def namespace(self, namespace_value):
        self.__namespace = namespace_value

    @property
    def command(self):
        return self.__command

    @command.setter
    def command(self, command_value):
        self.__command = command_value

    @property
    def labels(self):
        return self.__labels

    @labels.setter
    def labels(self, labels_value):
        self.__labels = labels_value

    @property
    def max_retrials(self):
        """
        Maximum number of retrials in case of failure.
        """
        return self.kubernetes_config.max_retrials

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
        return self.__active_deadline_seconds

    @active_deadline_seconds.setter
    def active_deadline_seconds(self, seconds):
        self.__active_deadline_seconds = seconds

    @property
    def kubernetes_config(self):
        if not self.__kubernetes_config:
            self.__init_kubernetes()
        return self.__kubernetes_config

    def __track_job(self):
        # """Poll job status while active"""
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

    def __get_job(self):
        api_response = self.__api_instance.list_namespaced_job(
            self.namespace, label_selector="luigi_task_id=" + self.job_uuid)
        jobs = api_response.items
        assert len(jobs) == 1, "Kubernetes job " + self.namespace +"/"+ self.uu_name + " not found" "(job_uuid= "+  self.job_uuid + ")"
        return jobs[0]

    def __get_pods(self):
        self.__logger.info(f"checking __get_pods")
        api_instance = kubernetes.client.CoreV1Api(
            kubernetes.client.ApiClient(self.kubernetes_config)
        )
        pods = api_instance.list_namespaced_pod(
            self.namespace, label_selector="job-name=" + self.uu_name
            )
        return pods.items

    def __get_pod_log(self, pod):
        api_response = ""
        try:
            api_instance = client.CoreV1Api(
                kubernetes.client.ApiClient(self.kubernetes_config)
            )
            api_response = api_instance.read_namespaced_pod_log(
                pod.metadata.name,
                self.namespace,
                pretty='true',
                timestamps='true')
            self.__logger.debug(api_response)
        except ApiException as e:
            print("Exception when calling CoreV1Api->read_namespaced_pod_log: %s\n" % e)
        return api_response

    def __print_pod_logs(self):
        for pod in self.__get_pods():
            self.__logger.info("Fetching logs from " + pod.metadata.name)
            logs = self.__get_pod_log(pod)
            if len(logs) > 0:
                for l in logs.split('\n'):
                    self.__logger.info(l)

    def __print_kubectl_hints(self):
        self.__logger.info("To stream Pod logs, use:")
        for pod in self.__get_pods():
            self.__logger.info("`kubectl logs -f pod/%s`" % pod.metadata.name)

    def __verify_job_has_started(self):
        # """Asserts that the job has successfully started"""
        # Verify that the job started
        job = self.__get_job()

        # Verify that the pod started
        pods = self.__get_pods()

        assert len(pods) > 0, "No pod scheduled by " + self.uu_name
        for pod in pods:
            status = pod.status
            if status.container_statuses:
                for cont_stats in status.container_statuses:
                    if cont_stats.state.terminated:
                        t = cont_stats.state.terminated
                        err_msg = "Pod %s %s (exit code %d). Logs: `kubectl logs pod/%s`" % (
                            pod.metadata.name, t.reason, t.exitCode, pod.metadata.name)
                        assert t.exitCode == 0, err_msg

                    if cont_stats.state.waiting:
                        wr = cont_stats.state.waiting.reason
                        assert wr == 'ContainerCreating', "Pod %s %s. Logs: `kubectl logs pod/%s`" % (
                            pod.metadata.name, wr, pod.metadata.name)
            if status.conditions:
                for cond in status.conditions:
                    if cond.message:
                        if cond.reason == 'ContainersNotReady':
                            return False
                        assert cond.status != 'False', \
                            "[ERROR] %s - %s" % (cond.reason, cond.message)
        return True

    def __get_job_status(self):
        """Return the Kubernetes job status"""
        # # Figure out status and return it
        job = self.__get_job()

        if job.status.succeeded:
            # job.scale(replicas=0)
            if self.print_pod_logs_on_exit:
                self.__print_pod_logs()
            if self.delete_on_success:
                self.__cleanup_finished_jobs(namespace=self.namespace, job=self.uu_name)
            return "SUCCEEDED"

        if job.status.failed:
            failed_cnt = job.status.failed
            self.__logger.debug("Kubernetes job " + self.uu_name
                                + " status.failed: " + str(failed_cnt))
            if self.print_pod_logs_on_exit:
                self.__print_pod_logs()
            if failed_cnt > self.max_retrials:
                return "FAILED"
        return "RUNNING"

    def __cleanup_finished_jobs(self, namespace='default', state='Finished', job=""):
        deleteoptions = client.V1DeleteOptions()
        try:
            jobs = self.__api_instance.list_namespaced_job(
                namespace,
                include_uninitialized=False,
                pretty=True,
                label_selector="luigi_task_id=" + job,
                timeout_seconds=60
            )
            # print(jobs)
        except ApiException as e:
            print("Exception when calling BatchV1Api->list_namespaced_job: %s\n" % e)

        # Now we have all the jobs, lets clean up
        # We are also logging the jobs we didn't clean up because they either failed or are still running
        for job in jobs.items:
            logging.debug(job)
            jobname = job.metadata.name
            jobstatus = job.status.conditions
            if job.status.succeeded == 1:
                # Clean up Job
                logging.info("Cleaning up Job: {}. Finished at: {}".format(
                    jobname, job.status.completion_time))
                try:
                    # What is at work here. Setting Grace Period to 0 means delete ASAP. Otherwise it defaults to
                    # some value I can't find anywhere. Propagation policy makes the Garbage cleaning Async
                    api_response = self.__api_instance.delete_namespaced_job(jobname,
                                                                    namespace,
                                                                    body=deleteoptions,
                                                                    grace_period_seconds=0,
                                                                    propagation_policy='Background')
                    logging.debug(api_response)
                except ApiException as e:
                    print(
                        "Exception when calling BatchV1Api->delete_namespaced_job: %s\n" % e)
            else:
                if jobstatus is None and job.status.active == 1:
                    jobstatus = 'active'
                logging.info("Job: {} not cleaned up. Current status: {}".format(
                    jobname, jobstatus))
        return

    def run(self):
        """Run the Luigi Pipeline task on Kubernetes Job
        """
        if self.runlocal:
            Task.run(self)
        else:
            self.__init_kubernetes()
            env_vars = self.env_vars
            job_spec = self.__create_job(
                self.uu_name,
                self.image,
                self.namespace,
                command=self.__command,
                env_vars=env_vars
            )
            # Submit job
            self.__logger.info("Submitting Kubernetes Job: " + self.uu_name)

            api_response = self.__api_instance.create_namespaced_job(
                self.namespace,
                job_spec,
                pretty=True
            )
            self.__logger.debug("Submitting Kubernetes Job: " +
                                self.uu_name + " Result: " + api_response.kind)

            # Track the Job (wait while active)
            self.__logger.info("Start tracking Kubernetes Job: " + self.uu_name)
            self.__track_job()

    def __create_job(self, name, container_image, namespace="luigi-jobs", container_name="luigi-jobcontainer", command=[], args=[], env_vars={}):
        """
        Create a k8 Job Object
        Minimum definition of a job object:
        {'api_version': None, - Str
        'kind': None,     - Str
        'metadata': None, - Metada Object
        'spec': None,     -V1JobSpec
        'status': None}   - V1Job Status
        Docs: https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1Job.md
        Docs2: https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/#writing-a-job-spec
        """
        labels = {
            "spawned_by": "luigi",
            "luigi_task_id": self.job_uuid
        }

        body = client.V1Job(api_version="batch/v1", kind="Job")
        body.metadata = client.V1ObjectMeta(namespace=namespace, name=name, labels=labels)
        body.status = client.V1JobStatus()
        template = client.V1PodTemplate()
        template.template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(name=self.uu_name))
        env_list = []

        for env_name, env_value in env_vars.items():
            env_list.append(client.V1EnvVar(name=env_name, value=env_value))

        container = client.V1Container(
            name=container_name,
            image=container_image,
            command=command,
            args=args,
            env=env_list
        )
        template.template.spec = client.V1PodSpec(
            containers=[container],
            restart_policy=self.restart_policy
        )
        # And finaly we can create our V1JobSpec!
        body.spec = client.V1JobSpec(
            ttl_seconds_after_finished=600,
            template=template.template
        )
        if self.active_deadline_seconds is not None:
            body.spec.active_deadline_seconds = self.__active_deadline_seconds
        return body

    @property
    def env_vars(self):
        """Get Carol environment variables and return as a dict object to this class
        TODO: This a mixing of code responsabilites, Carol and Luigi, we have to thing about to segregate luigi code
        from Carol.ai code

        Returns:
            dict -- A dict object containing all default Carol enviroment variables.
        """
        return {
            "CAROLCONNECTORID": os.environ.get('CAROLCONNECTORID', ''),
            "CAROLAPPOAUTH": os.environ.get('CAROLAPPOAUTH', ''),
            "LONGTASKID": os.environ.get('LONGTASKID', ''),
            "CAROLTENANT": os.environ.get('CAROLTENANT', ''),
            "CAROLAPPNAME": os.environ.get('CAROLAPPNAME', ''),
            "IMAGE_NAME": os.environ.get('IMAGE_NAME', '')
        }

if __name__ == "__main__":
    pass
