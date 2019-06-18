from pycarol.luigi_extension.taskviewer.test.pipeline_example import pipeline1

def test_get_plot():
    from pycarol.luigi_extension.taskviewer.bokeh_plot import get_plot_from_pipeline
    plot = get_plot_from_pipeline(pipeline1)
    assert plot
