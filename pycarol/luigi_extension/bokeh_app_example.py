
# bokeh serve --show bokeh_app_example.py
#carol-ds-education example
from app.flow.commons import params
from luigi_extension import TaskViewer
from app.flow.output import RunMe
from bokeh.plotting import curdoc

TV = TaskViewer(RunMe(**params))

TV.plot_task(curdoc(),web_app=True)