
class ManifestValidator():

    def __init__(self, manifest_json):
        self.manifest_json = manifest_json


    def run(self):
        status, message = self._basic_processes('batch')
        if status is False:
            return message

        status, message = self._basic_processes('online')
        if status is False:
            return message

        return ''


    def _basic_processes(self, process_type):
        if process_type in self.manifest_json:
            for process in self.manifest_json[process_type]['processes']:
                if 'name' not in process:
                    return False, '"name" not found in some "{}" process'.format(process_type)
                if 'algorithmName' not in process:
                    return False, '"algorithmName" not found in the "{}" process called "{}"'.format(process_type, process['name'])
                if 'algorithmTitle' not in process:
                    return False, '"algorithmTitle" not found in the "{}" process called "{}"'.format(process_type, process['name'])
                if process_type == 'online' and 'properties' not in process['instanceProperties']:
                    return False, '"properties" not found in the "{}" process called "{}"'.format(process_type, process['name'])

                if 'git' in process:
                    for git in process['git']:
                        if "type" not in git:
                            return False, '"type" not found in the "git". "{}" process called "{}"'.format(process_type, process['name'])
                        if "url" not in git:
                            return False, '"url" not found in the "git". "{}" process called "{}"'.format(process_type, process['name'])
                        if "destinationFolder" not in git:
                            return False, '"destinationFolder" not found in the "git". "{}" process called "{}"'.format(process_type, process['name'])
                        if "branch" in git and git['branch'] == '':
                            return False, '"branch" is empty in the "git". "{}" process called "{}"'.format(process_type, process['name'])
        return True, ''
