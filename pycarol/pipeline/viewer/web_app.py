# bokeh serve --show web_app.py

from pycarol.pipeline.viewer.test.pipeline_example import pipeline1

from bokeh.plotting import curdoc
doc = curdoc()
from pycarol.pipeline.viewer.bokeh_plot import get_plot_from_pipeline
plot = get_plot_from_pipeline(pipeline1)
doc.add_root(plot)