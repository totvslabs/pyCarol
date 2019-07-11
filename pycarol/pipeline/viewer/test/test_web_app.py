from pycarol.pipeline.tools.pipeline_example import pipeline1

def test_get_plot():
    from pycarol.pipeline.viewer.bokeh_plot import get_plot_from_pipeline
    plot = get_plot_from_pipeline(pipeline1)
    assert plot


def test_bokeh_doc():
    from pycarol.pipeline.tools.pipeline_example import pipeline1
    from pycarol.pipeline.viewer.bokeh_plot import get_plot_from_pipeline
    plot = get_plot_from_pipeline(pipeline1)
    from bokeh.plotting import curdoc    
    doc = curdoc()

