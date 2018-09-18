class Singleton(type):
    _instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instance


class KeySingleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if len(args) == 0:
            key = ''
        else:
            key = args[0]
        if key not in cls._instances:
            cls._instances[key] = super(KeySingleton, cls).__call__(*args, **kwargs)
        return cls._instances[key]
