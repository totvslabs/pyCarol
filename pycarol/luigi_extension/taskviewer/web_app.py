# bokeh serve --show taskviewer_app.py

from pycarol.luigi_extension.task import Task, inherit_list


class T1(Task):
    pass


@inherit_list(T1)
class T2(Task):
    pass


@inherit_list(T1, T2)
class T3(Task):
    pass


@inherit_list(T2, T3)
class T4(Task):
    def easy_run(self, inputs):
        raise Exception


@inherit_list(T1, T3)
class T5(Task):
    pass


params = {}

from bokeh.plotting import curdoc
doc = curdoc()
from .bokeh_plot import get_plot_from_task
plot = get_plot_from_task([T5(**params)])
doc.add_root(plot)