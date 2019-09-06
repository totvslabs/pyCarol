import dash
import dash_core_components as dcc
from dash.dependencies import Input,Output,State
import dash_html_components as html
import dash_cytoscape as cyto
import json
cyto.load_extra_layouts()
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

def get_app_from_pipeline(pipe):
    app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
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
                'opacity': 0.9,

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
                'selectable': True,
            }
        }
        for i,family in enumerate(
            {t.get_task_family().split('.')[-2] for t in dag.keys()}
        )
    ]
    default_stylesheet = group_selectors + class_selectors


    def row(*children):
        return html.Div(className='row',children=list(children))

    def column(*children,size=6,**params):
        class_name_dict = {
            6: "six columns"
        }
        return html.Div(className=class_name_dict[size],children=list(
            children),**params)


    main_pipeline = cyto.Cytoscape(
        id='main_pipeline',
        layout=dict(
            name='dagre',
            # rankDir='RL',
            # ranker='longest-path',
            padding=20,
            nodeDimensionsIncludeLabels=True,
            animate=True,
            animationDuration=2000,
        ),
        style={
            'width':'100%',
            'height':'600px',
            'background-color': '#fdfd96',
        },
        stylesheet=default_stylesheet,
        elements=nodes_elements + edges_elements,
    )
    tap_node_data = html.P(
        id='tap_node_data',
        style={'background-color': '#fdfd96'},
    )
    print_node_button = html.Button(
        id='print_node_button',
        children='Print Node',

    )
    print_node_data = html.P(
        id='print_node_data',
        style={'background-color': '#fdfd96'},
    )

    app.layout = row(
        column(main_pipeline),
        column(tap_node_data,print_node_button,print_node_data),
    )

    def dump_json(node):
        if node:
            return json.dumps(node,indent=2)
    def print_node(n_clicks,selected_nodes):

        output = json.dumps(selected_nodes)
        print("###",n_clicks,'|',selected_nodes)
        return output

    def modify_selected_stylesheet(selected_node_data):
        if selected_node_data is None:
            return default_stylesheet
        specific_stylesheet = [
            {
                'selector': 'nodes',#f"[ id == {node_data['id']}]",
                'style': {
                    'background-color': 'red'
                },
            }
            # for node_data in selected_node_data
        ]
        return specific_stylesheet + default_stylesheet

    print(print_node_data.id)
    app.callback(
        Output(tap_node_data.id, 'children'),
        [Input(main_pipeline.id, 'selectedNodeData')]
    )(dump_json)

    app.callback(
        Output(print_node_data.id, 'children'),
        [Input(print_node_button.id, 'n_clicks')],
        [State(main_pipeline.id, 'selectedNodeData')],
    )(print_node)

    app.callback(
        Output('main_pipeline', 'stylesheet'),
        [Input('main_pipeline', 'selectedNodeData')]
    )(modify_selected_stylesheet)

    return app
