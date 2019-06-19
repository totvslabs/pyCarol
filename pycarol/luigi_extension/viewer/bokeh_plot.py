import bokeh
assert bokeh.__version__ == '1.2.0'
from bokeh.models import (
    ColumnDataSource,
    CDSView,
    BooleanFilter,
    HoverTool,
)

from bokeh.events import ButtonClick
from bokeh.models import Button
from bokeh.layouts import widgetbox
from bokeh.models.widgets import PreText

from bokeh.layouts import column, row, layout
from bokeh.plotting import figure
from bokeh.transform import transform
from bokeh.palettes import Category10
from bokeh.models.mappers import CategoricalColorMapper

def _make_colormapper(data_source: dict, col_name: str):
    import numpy as np
    factors = list(set(data_source[col_name]))
    nb_factors = np.clip(len(factors), 3, 10)
    return CategoricalColorMapper(factors=factors,
                                  palette=Category10[nb_factors])


def _make_pipeline_plot(
        nodes_data_source,
        edges_data_source,
):

    family_color = _make_colormapper(nodes_data_source.data,'task_family')

    pipeline_plot = figure(
        title="Pipeline Debugger",
        x_range=(-1, max(nodes_data_source.data['x']) + 1),
        y_range=(-1, max(nodes_data_source.data['y']) + 1),
        tools=[
            HoverTool(names=['alltasks']),
            'wheel_zoom',
            'pan',
            'tap',
            'box_select',
            'reset'
        ],
        toolbar_location='right',
        tooltips=[
            ('index', "$index"),
            ('task_id', '@task_id'),
        ],
        plot_width=900,
        plot_height=500,
    )

    edges_glyph = pipeline_plot.segment(
        'x0',
        'y0',
        'x1',
        'y1',
        source=edges_data_source,
        color='gray',
        line_alpha=0.3,
    )

    pipeline_plot.circle(
        'x',
        'y',
        source=nodes_data_source,
        size=20,
        name='alltasks',
        color=transform('task_family', family_color),
        legend='task_family',
    )

    # pipeline_plot.circle(
    #     'x',
    #     'y',
    #     source=s2,
    #     size=10,
    #     color='white',
    # )

    pipeline_plot.text(
        x='x',
        y='y',
        text='task_name',
        source=nodes_data_source,
        text_font_size='8pt',
    )

    edges_glyph.nonselection_glyph = None
    return pipeline_plot

class PlotDynamics():
    def __init__(
            self,
            nodes_data_source,
            edges_data_source,
    ):
        nodes_data_source.selected.on_change(
            'indices', self.select_callback
        )

        self.remove_button = Button(label='Remove Selected')
        self.removeupstream_button = Button(label='Remove Upstream')
        self.update_button = Button(label='Update')

        self.remove_button.on_event(
            ButtonClick,
            self.remove_callback,
        )
        self.removeupstream_button.on_event(
            ButtonClick,
            self.removeupstream_callback,
        )
        self.update_button.on_event(
            ButtonClick,
            self.update_callback,
        )

        self.nodes_data_source = nodes_data_source
        self.edges_data_source = edges_data_source
        self.selected_nodes = []


    def select_callback(self,attr, old, new):
        self.selected_nodes = new
        # pre.text = self.tasklog[new[0]]
        print("selected {}".format(new))

    def remove_callback(self,event):
        return

        # target_list = self.nodes_data_source.selected.indices
        # for t in target_list:
        #     print('removing', t)
        #     task = self.
        #     TaskViewer.nodes[t].remove()

    def removeupstream_callback(self,event):
        return

    def update_callback(event):
        # complete = self.update_complete()
        # s2.data = {k: [vi for i, vi in enumerate(v) if not complete[i]] for
        #            k, v in source_dict.items()}
        return
    def buttons(self):
        return row([
            self.remove_button,
            self.update_button,
            self.removeupstream_button,
        ])

def plot_pipeline(nodes_data,edges_data,pipe):
    """
    Receives data sources in dict format and returns dynamic plot
    Args:
        nodes_data:
        edges_data:

    Returns:
        final_layout:

    """

    nodes_data_source = ColumnDataSource(data=nodes_data)
    edges_data_source = ColumnDataSource(data=edges_data)

    # done_tasks_data_source = CDSView(
    #     source=nodes_data_source,
    #     filters=[BooleanFilter(complete)]
    # )
    # notdone_tasks_data_source = CDSView(
    #     source=nodes_data_source,
    #     filters=[BooleanFilter([not c for c in complete])]
    # )

    pipeline_plot = _make_pipeline_plot(nodes_data_source, edges_data_source)

    dynamics = PlotDynamics(nodes_data_source,edges_data_source)

    ### Layout
    final_layout = layout(
        column(
            [
                pipeline_plot,
                row(dynamics.buttons()),
            ],
            sizing_mode='scale_width'
        ),
    )
    return final_layout


def get_plot_from_pipeline(pipe):
    """
    Main module method. From a luigi task with defined parameters, generates a
    bokeh plot of the
    pipeline. It does not render the plot.
    Args:
        pipe: list of luigi task initialised with proper parameters

    Returns:
        bokeh_layout:

    """

    from .viewer import (nodes_layout, edges_layout,
                         make_nodes_data_source, make_edges_data_source,
                         )

    dag = pipe.get_dag()

    nodes_layout = nodes_layout(dag)
    edges_layout = edges_layout(dag, nodes_layout)

    nodes_data_source = make_nodes_data_source(nodes_layout)
    edges_data_source = make_edges_data_source(edges_layout)

    bokeh_layout = plot_pipeline(nodes_data_source, edges_data_source,pipe)

    return bokeh_layout



