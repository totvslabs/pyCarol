import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_cytoscape as cyto
cyto.load_extra_layouts()
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

def get_app_from_pipeline(pipe):
    from pycarol.pipeline.viewer.viewer import (nodes_layout, edges_layout, )

    app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
    root_tasks = pipe.top_nodes
    dag = pipe.get_dag()

    nodes_elements = [
        {
            'data': {
                'id': k.task_id,
                'label': k.get_task_family().split('.')[-1],
            },
            'classes': k.get_task_family().split('.')[-2],
        }
        for i,k in enumerate(dag.keys())
    ]

    edges_elements =[
        {
            'data': {
                'source': source.task_id,
                'target': target.task_id,
            }
        }
        for i,source in enumerate(dag.keys()) for target in dag[source]
    ]

    group_selectors = [
        {
            'selector': 'node',
            'style': {
                'font-size': '12px',
                'content': 'data(label)',
                'shape': 'oval',
            }
        },
        {
            'selector': 'edge',
            'style': {
                'curve-style': 'bezier',
                'source-arrow-shape': 'vee',
                'arrow-scale': 2,
                # 'line-color': 'blue'
            }
        }
    ]
    import colorcet as cc

    class_selectors = [
        {
            'selector': '.' + family,
            'style': {
                'background-color': cc.glasbey_dark[i],
            }
        }
        for i,family in enumerate(
            {t.get_task_family().split('.')[-2] for t in dag.keys()}
        )
    ]
    my_stylesheet = group_selectors + class_selectors




    app.layout = html.Div(children=[

        html.H1(children='Pipeline viewer'),

        cyto.Cytoscape(
            id='main-pipeline',
            layout=dict(
                name='dagre',
                # rankDir='RL',
                # ranker='longest-path',
                padding=100,
                nodeDimensionsIncludeLabels=True,
                animate=True,
                animationDuration=2000,
            ),
            style=dict(width='100%', height='600px'),
            stylesheet=my_stylesheet,
            elements=nodes_elements+edges_elements,

        )

    ])

    return app
