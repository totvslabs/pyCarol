import dash
import dash_core_components as dcc
from dash.dependencies import Input,Output,State
import dash_html_components as html
from dash_html_components import Button, Div
import dash_cytoscape as cyto
import json
cyto.load_extra_layouts()
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

def get_app_from_pipeline(pipe):
    app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
    dag = pipe.get_dag()

    def get_nodes_elements():
        pipe.update_all_complete_status()
        return [
            {
                'data': {
                    'id': k.task_id,
                    'label': k.get_task_family().split('.')[-1],
                    'complete': pipe.get_task_complete(k),
                    'i': i,
                },
                'classes': k.get_task_family().split('.')[-2],
            }
            for i,k in enumerate(dag.keys())
        ]
    def get_edges_elements():
        return [
            {'data': {'source': source.task_id, 'target': target.task_id, }} for
            i, source in enumerate(dag.keys()) for target in dag[source]
        ]


    nodes_elements = get_nodes_elements()
    edges_elements = get_edges_elements()

    group_selectors = [
        {
            'selector': 'node',
            'style': {
                'font-size': '18',
                'content': 'data(label)',
                'shape': 'ellipse',
                'opacity': 1.0,
                'border-style': 'solid',
                'border-width': 5,

            }
        },

        {
            'selector': 'edge',
            'style': {
                'curve-style': 'bezier',
                'source-arrow-shape': 'vee',
                'arrow-scale': 2,
            }
        }
    ]
    import colorcet as cc

    class_selectors = [
        {
            'selector': '.' + family,
            'style': {
                'background-color': cc.glasbey_dark[i],
                'border-color': cc.glasbey_dark[i],
                'selectable': True,
            }
        }
        for i,family in enumerate(
            {t.get_task_family().split('.')[-2] for t in dag.keys()}
        )
    ]
    complete_selector =[
        {
            'selector': "[!complete]",
            'style': {
                'background-color': 'white',
                'shape': 'round-rectangle',
            }
        }
    ]
    default_stylesheet = group_selectors + class_selectors + complete_selector


    def row(*children):
        return html.Div(className='row',children=list(children))

    def column(*children,size=12,**params):
        class_name_dict = {
            6: "six columns",
            12: "twelve columns",
        }
        return html.Div(className=class_name_dict[size],children=list(
            children),**params)


    main_pipeline = cyto.Cytoscape(
        id='main_pipeline',
        layout=dict(
            name='dagre',
            # rankDir='RL',
            # ranker='longest-path',
            padding=5,
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

    tap_node_data = Div(
        id='tap_node_data_div',
        children=[
            html.P(id="tap_node_header", children="Selected Nodes"),
            html.P(id='tap_node_data'),
        ],
        style={'background-color': '#fdfd96'},
    )

    run_button = Button(id='run_button',children='Run')
    remove_button = Button(id='remove_button',children='Remove')
    remove_upstream_button = Button(id='remove_upstream_button',
                                    children='Remove Upstream')
    update_button = Button(id='update_button',children='Update')


    buttons_list = [update_button,run_button,remove_button,
                    remove_upstream_button]

    buttons_output = [
        html.H3(
            id='buttons_output_header',
            children='Buttons Output',
        ),
        html.P(
            id='buttons_output',
        )
        ]

    selected_output = [
        html.H3(
            id='selected_output_header',
            children='Selected Nodes',
        ),
        html.P(
            id='selected_output',
        ),
        ]


    app.layout = column(
        row(main_pipeline),
        row(
            column(
                row(*buttons_list),
                column(*buttons_output, size=6,),
                size=6,
                style={'background-color': '#fdfd96'},
            ),
            column(*selected_output,size=6,style={'background-color': '#fdfd96'},)
        ),
    )

    ### Dynamics ###
    def select_callback(*ts_list):
        max_ts = -1
        print(ts_list)
        for i,ts in enumerate(ts_list):
            if ts and ts>max_ts:
                max_ts = ts
                cb_i = i
        if max_ts == -1:
            return None
        else:
            return cb_i

    def cb_run(nodes):
        if len(nodes) == 0:
            return "No task selected."
        if len(nodes) > 1:
            return "Please, select only one task to run."
        task_id = nodes[0]['id']
        task = pipe.get_task_by_id(task_id)
        # task.run()
        return f"Fake Run {task_id}"

    def cb_goto(nodes):
        pass

    def cb_remove(nodes):
        for n in nodes:
            task_id = n['id']
            task = pipe.get_task_by_id(task_id)
            task.remove()
        return "Removing:\n" + json.dumps(nodes)

    def cb_remove_upstream(nodes):
        tasks = [pipe.get_task_by_id(n['id']) for n in nodes]
        pipe.remove_upstream(tasks)
        return "Removing upstream from:\n" + json.dumps(nodes)

    def cb_dummy(*args,**kwargs):
        return "Updating complete targets."

    @app.callback(
        Output('buttons_output', 'children'),
        [
            Input(b.id, 'n_clicks_timestamp')
            for b in buttons_list
        ],
        [State(main_pipeline.id, 'selectedNodeData')],
    )
    def callback_buttons(*inputs):
        callback_list = [cb_dummy,cb_run,cb_remove,cb_remove_upstream]
        assert len(callback_list) == len(buttons_list)
        ts_list = inputs[0:-1]
        selected_nodes = inputs[-1]
        cb_i = select_callback(*ts_list)
        if cb_i is None:
            return None
        else:
            return callback_list[cb_i](nodes=selected_nodes)

    @app.callback(
        Output('selected_output', 'children'),
        [Input(main_pipeline.id, 'selectedNodeData')],
    )
    def cd_select_node(nodes):
        output = []
        if not nodes:
            return "No task selected."
        for n in nodes:
            task_id = n['id']
            task = pipe.get_task_by_id(task_id)
            task_address = task.get_task_address()
            if task_address is None:
                output.append(task.get_task_family())
            else:
                output.append(
                    dcc.Link(
                        task.get_task_family(),
                        href='/tree/' + task_address,
                    )
                )
            output.append(" ")
        return output

    @app.callback(
        Output(main_pipeline.id, 'elements'),
        [Input(update_button.id, 'n_clicks_timestamp')]
    )
    def cb_update(ts):
        print("cb_update")
        return get_nodes_elements() + get_edges_elements()

    return app
