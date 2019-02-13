

class Visualization:
    """ Main class to control Tasks visualizations
    """

    def __call__(self, *args, **kwargs):
        raise NotImplementedError('No visualization defined for that task.')

    def __init__(self, task=None):
        self._task = task
