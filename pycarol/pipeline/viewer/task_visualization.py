

class Visualization:
    """ Main class to control Tasks visualizations

    This class is intended to provide the base for Carol pipelines visualization.
    It uses bokeh as a backend and provides some common plots that can be used.

    This class can be extended to provide also user specific visualizations.

    E.g.

    MyVisualization(Visualization):
        def __call__(*args, **kwargs):
            do my plot...

        def plot_another_thing(...):
            do other plot...

    and in a pipeline task:

    MyTask(Task):
        visualization_class = MyVisualization

    """

    def __call__(self, *args, **kwargs):
        raise NotImplementedError('No visualization defined for that task.')

    def __init__(self, task=None):
        self._task = task

    def null_data(self, data=None, *args, **kwargs):
        import missingno as msno
        msno.matrix(df=data, *args, **kwargs)
