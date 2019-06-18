# bokeh serve --show taskviewer_app.py

from app.flow.commons import params
from pycarol.luigi_extension import TaskViewer
from app.flow.output import RunMe
from bokeh.plotting import curdoc

TV = TaskViewer(RunMe(**params))

TV.plot_task(curdoc(), web_app=True)
