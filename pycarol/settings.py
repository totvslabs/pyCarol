from pycarol.utils import KeySingleton
from pycarol.apps import Apps


class Settings(metaclass=KeySingleton):
    def __init__(self, carol):
        self.carol = carol
        self.settings = Apps(carol).get_by_name(self.carol.app_name)

    def get(self, key):
        return self.settings[key]

    def all(self):
        return self.settings
