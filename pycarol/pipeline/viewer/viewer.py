
def load_pipeline():
    try:
        import app.pipeline
    except ModuleNotFoundError as e:
        print(
            "Module app.pipeline was not found. Please, pip install your app "
            "and define your pipeline in app/pipeline.py")
        raise e

    from importlib import reload
    app_pipeline = reload(app.pipeline)

    from pycarol.pipeline.tools import Pipe

    try:
        pipeline = app_pipeline.pipeline
        assert isinstance(pipeline, Pipe)
    except AttributeError as e:
        print("Please define the object pipeline in app/pipeline.py")
        raise e
    except AssertionError as e:
        print(
            "pipeline object in app/pipeline.py should be an instance of from "
            "pycarol.pipeline.tools.Pipe class")
        raise e

    return pipeline


def show_pipeline():
    pipeline = load_pipeline()
    from pycarol.pipeline.viewer.dash_plot import get_app_from_pipeline
    app = get_app_from_pipeline(pipeline)

    import jupyterlab_dash
    import dash
    import dash_html_components as html
    viewer = jupyterlab_dash.AppViewer()
    viewer.show(app)