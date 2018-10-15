import json

class ManifestParser():

    def __init__(self, manifest_json):
        self.manifest_json = manifest_json


    def has_batch_process(self):
        return 'batch' in self.manifest_json and self.manifest_json['batch'] != {}


    def has_batch_process_name(self, name):
        if self.has_batch_process():
            for process in self.manifest_json['batch']['processes']:
                if process['name'] == name:
                    return process
        return None


    def has_online_process(self):
        return 'online' in self.manifest_json and self.manifest_json['online'] != {}


    def has_online_process_name(self, name):
        if self.has_online_process():
            for process in self.manifest_json['online']['processes']:
                if process['name'] == name:
                    return process
        return None


    def has_instance_properties_batch(self, name):
        process = self.has_batch_process_name(name)
        if process:
            if 'instanceProperties' in process and process['instanceProperties'] != {}:
                return True
        return False


    def chosen_profile_batch(self, name):
        process = self.has_batch_process_name(name)
        if process:
            chosenProfile = process['instanceProperties']['profile']
            return 'default' if chosenProfile == '' else chosenProfile
        return None


    def keep_instance_batch(self, name):
        process = self.has_batch_process_name(name)
        if process and 'keepInstance' in process['instanceProperties']:
            keepInstance = process['instanceProperties']['keepInstance']
            return True if keepInstance == 'True' or keepInstance == 'true' else False
        return False


    def docker_image_batch(self, name):
        process = self.has_batch_process_name(name)
        if process:
            return process['instanceProperties']['properties']['dockerImage']
        return None


    def instance_memory_batch(self, name):
        process = self.has_batch_process_name(name)
        if process:
            return process['instanceProperties']['properties']['instanceMemory']
        return None


    def instance_so_batch(self, name):
        process = self.has_batch_process_name(name)
        if process:
            return process['instanceProperties']['properties']['instanceSO']
        return None


    def instance_size_batch(self, name):
        process = self.has_batch_process_name(name)
        if process:
            return process['instanceProperties']['properties']['instanceSize']
        return None


    def instance_vcpus_batch(self, name):
        process = self.has_batch_process_name(name)
        if process:
            return process['instanceProperties']['properties']['instanceVCPUs']
        return None


    def luigi_workers_batch(self, name):
        process = self.has_batch_process_name(name)
        if process and 'luigi' in process['instanceProperties']:
            if process and 'workers' in process['instanceProperties']['luigi']:
                return process['instanceProperties']['luigi']['workers']
        return '2'


    def instance_environment_variables_batch(self, name):
        process = self.has_batch_process_name(name)
        if process and 'environments' in process['instanceProperties']:
            return process['instanceProperties']['environments']
        return None


    def algorithm_description_batch(self, name):
        process = self.has_batch_process_name(name)
        if process:
            if 'algorithmDescription' in process:
                if 'pt-br' in process:
                    return process['algorithmDescription']['pt-br']
                elif 'en-us' in process:
                    return process['algorithmDescription']['en-us']
        return ''


    def algorithm_name_batch(self, name):
        process = self.has_batch_process_name(name)
        if process:
            return process['algorithmName']
        return None


    def algorithm_method_batch(self, name):
        process = self.has_batch_process_name(name)
        if process:
            return process['algorithmMethod']
        return None


    def algorithm_title_batch(self, name):
        process = self.has_batch_process_name(name)
        if process:
            if 'algorithmTitle' in process:
                if 'pt-br' in process:
                    return process['algorithmTitle']['pt-br']
                elif 'en-us' in process:
                    return process['algorithmTitle']['en-us']
        return ''


    def git_batch(self, name):
        process = self.has_batch_process_name(name)
        if process and 'git' in process:
            return process['git']
        return None


    def has_instance_properties_online(self, name):
        process = self.has_online_process_name(name)
        if process:
            if 'instanceProperties' in process and process['instanceProperties'] != {}:
                return True
        return False


    def docker_image_online(self, name):
        process = self.has_online_process_name(name)
        if process:
            return process['instanceProperties']['properties']['dockerImage']


    def instance_memory_online(self, name):
        process = self.has_online_process_name(name)
        if process:
            return process['instanceProperties']['properties']['instanceMemory']


    def instance_so_online(self, name):
        process = self.has_online_process_name(name)
        if process:
            return process['instanceProperties']['properties']['instanceSO']


    def instance_size_online(self, name):
        process = self.has_online_process_name(name)
        if process:
            return process['instanceProperties']['properties']['instanceSize']
        return None


    def instance_vcpus_online(self, name):
        process = self.has_online_process_name(name)
        if process:
            return process['instanceProperties']['properties']['instanceVCPUs']
        return None


    def luigi_workers_online(self, name):
        process = self.has_online_process_name(name)
        if process and 'luigi' in process['instanceProperties']:
            return process['instanceProperties']['luigi']['workers']
        return None


    def instance_environment_variables_online(self, name):
        process = self.has_online_process_name(name)
        if process and 'environments' in process['instanceProperties']:
            return process['instanceProperties']['environments']
        return None


    def algorithm_description_online(self, name):
        process = self.has_online_process_name(name)
        if process:
            if 'algorithmDescription' in process:
                if 'pt-br' in process:
                    return process['algorithmDescription']['pt-br']
                elif 'en-us' in process:
                    return process['algorithmDescription']['en-us']
        return ''


    def name_online(self, name):
        process = self.has_online_process_name(name)
        if process:
            return process['name']
        return None


    def algorithm_name_online(self, name):
        process = self.has_online_process_name(name)
        if process:
            return process['algorithmName']
        return None


    def algorithm_title_online(self, name):
        process = self.has_online_process_name(name)
        if process:
            if 'algorithmTitle' in process:
                if 'pt-br' in process:
                    return process['algorithmTitle']['pt-br']
                elif 'en-us' in process:
                    return process['algorithmTitle']['en-us']
        return ''


    def git_online(self, name):
        process = self.has_online_process_name(name)
        if process and 'git' in process:
            return process['git']
        return None