from .utils.singleton import KeySingleton
from .apps import Apps


class Settings(metaclass=KeySingleton):
    def __init__(self, carol):
        self.carol = carol
        self.app = Apps(carol).get_settings(app_name=self.carol.app_name)

    def get(self, key):
        return self.app.appSettings[key]

    def get_full(self, key):
        return self.app.fullSettings[key]

    def all(self):
        return self.app.appSettings

    def all_full(self):
        return self.app.fullSettings
