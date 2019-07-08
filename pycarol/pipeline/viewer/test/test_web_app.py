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

if False:#WIP
    #temporarily disable possibly blocking test
    def test_start_app():
        # This test is in the context of searching an easy way to debug bokeh apps
        from tornado.ioloop import IOLoop

        from bokeh.application.handlers import FunctionHandler
        from bokeh.application import Application
        from bokeh.server.server import Server

        from pycarol.pipeline.tools.pipeline_example import pipeline1
        from pycarol.pipeline.viewer.bokeh_plot import get_plot_from_pipeline
        plot = get_plot_from_pipeline(pipeline1)
        modify_doc = lambda doc: doc.add_root(plot)

        io_loop = IOLoop.current()
        bokeh_app = Application(FunctionHandler(modify_doc))

        server = Server({'/': bokeh_app}, io_loop=io_loop)

        server.start()

        io_loop.add_callback(server.show, "/")
        io_loop.add_callback(io_loop.stop)
        io_loop.start()
        io_loop.close()
