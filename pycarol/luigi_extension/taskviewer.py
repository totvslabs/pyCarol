import numpy as np
import pandas as pd
from bokeh.io.state import curstate
from bokeh.io.notebook import show_app
from bokeh.io import output_notebook

output_notebook()

class TaskViewer(object):
    nodes = []
    selected_nodes = []

    def __init__(self, task, update_period=30,parallel_update=False):
        self.dag = dict()
        # TaskViewer.nodes = []
        self.node_level = dict()
        self.task = task
        self.tree = self.get_tree(task)
        self.max_level = max({self.node_level[k] for k in self.node_level.keys()})
        self.root_nodes = [i for i, v in self.node_level.items() if v == self.max_level]
        self.dag_node_level = [0 for i in self.node_level]
        for i in self.range_nodes():
            self.set_dag_node_level(i, 0)
        self.rev_dag = self.get_reverse_dag()
        self.update_period = update_period # seconds
        self.parallel_update = parallel_update

    def range_nodes(self):
        return range(len(TaskViewer.nodes))

    def get_tree(self, t, level=0):
        if t not in TaskViewer.nodes:
            TaskViewer.nodes.append(t)
        t_i = TaskViewer.nodes.index(t)
        if t_i not in self.node_level:
            self.node_level[t_i] = level
        sons = t.requires()
        for son in sons:
            if son not in TaskViewer.nodes:
                TaskViewer.nodes.append(son)
            son_i = TaskViewer.nodes.index(son)
            if son_i not in self.dag:
                self.dag[son_i] = {t_i}
            else:  # son already in dag
                self.dag[son_i].add(t_i)
            self.get_tree(son, level + 1)


    def set_dag_node_level(self, node, level=0):
        self.dag_node_level[node] = max(level, self.dag_node_level[node])
        if node not in self.dag:
            return
        for n_i in self.dag[node]:
            self.set_dag_node_level(n_i, level + 1)

    def get_reverse_dag(self):
        rd = {k: [] for k in self.range_nodes()}
        for source_node, v in self.dag.items():
            for target_node in v:
                rd[target_node].append(source_node)
        return rd

    def get_layout(self):
        x = []
        y = []
        temp_level = dict()
        node_level = self.dag_node_level
        for i in self.range_nodes():
            _x = node_level[i]
            x.append(_x)
            if node_level[i] not in temp_level:
                temp_level[node_level[i]] = 0
            else:
                temp_level[node_level[i]] += 1
            _y = temp_level[node_level[i]] + ((-_x) % 2) / 2. + np.random.random() * 0.5
            y.append(_y)
        return x, y

    def get_edges(self, x, y):
        edges_x = []
        edges_y = []
        rev_dag = self.get_reverse_dag()

        for source_node in self.range_nodes():
            l = [target_node for target_node in rev_dag[source_node]]
            result = [source_node]
            for e in l:
                result.append(e)
                result.append(source_node)
            edges_x.append([x[r] for r in result])
            edges_y.append([y[r] for r in result])
        return edges_x, edges_y

    def get_source_dict(self):
        x, y = self.get_layout()
        edges_x, edges_y = self.get_edges(x, y)

        task_names = []
        task_id = []
        for i in self.range_nodes():
            task_names.append(TaskViewer.nodes[i].task_family)
            task_id.append(TaskViewer.nodes[i].task_id)
        family = [t.split('.')[0] for t in task_names]
        task_names = [t.split('.')[1] for t in task_names]
        self.family = family
        self.update_complete()
        source_dict = dict(
            x=x,
            y=y,
            task_names=task_names,
            family=family,
            task_id=task_id,
            edges_x=edges_x,
            edges_y=edges_y,
            complete=self.complete,
        )
        return source_dict

    def update_complete(self):
        if self.parallel_update:
            with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
                complete = executor.map(lambda x: x.complete(), TaskViewer.nodes)
            self.complete = list(complete)
        else:
            self.complete = [t.complete() for t in TaskViewer.nodes ]
        return self.complete

    def get_colormapper(self):
        family = self.family
        factors = list(set(family))
        nb_factors = len(factors)
        from bokeh.palettes import Category10
        from bokeh.models.mappers import CategoricalColorMapper
        return CategoricalColorMapper(factors=factors, palette=Category10[nb_factors])

    def plot_task(self, doc):
        # plot dashboard
        print('start plot_task')
        doc.clear()
        from bokeh.models import Circle, Text, Button
        from bokeh.models import ColumnDataSource, CDSView, BooleanFilter
        from bokeh.plotting import figure
        from bokeh.layouts import column, row, layout
        from bokeh.transform import transform

        source_dict = self.get_source_dict()
        cm = self.get_colormapper()

        TOOLTIPS = [
            ('index', "$index"),
            ('task_id', '@task_id'),
        ]

        plot = figure(
            title="Pipeline Debugger",
            x_range=(-1, max(source_dict['x']) + 1),
            y_range=(-1, max(source_dict['y']) + 1),
            tools=['hover', 'wheel_zoom', 'pan', 'tap', 'box_select', 'reset'],
            toolbar_location='right',
            tooltips=TOOLTIPS,
            plot_width=900,
            plot_height=800,
        )

        s1 = ColumnDataSource(data=source_dict)
        #         s1_edges = ColumnDataSource(data=source_dict)
        complete = self.update_complete()
#         print(complete)
        s2 = ColumnDataSource(data={k: [vi for i, vi in enumerate(v) if not complete[i]] for k, v in
                                    source_dict.items()})  # select only the rows of source_dict where complete==False
        done_tasks = CDSView(source=s1, filters=[BooleanFilter(complete)])
        notdone_tasks = CDSView(source=s1, filters=[BooleanFilter([not c for c in complete])])

        plot.multi_line(xs='edges_x', ys='edges_y', source=s1, color='gray')
        plot.circle('x', 'y', source=s1, size=20, color=transform('family', cm), legend='family')
        plot.circle('x', 'y', source=s2, size=10, color='white')
        plot.text(x='x', y='y', text='task_names', source=s1, text_font_size='8pt')

        from bokeh.events import ButtonClick
        from bokeh.models import Button
        from bokeh.models.callbacks import CustomJS

        def select_callback(attr, old, new):
#             print('hey',new)
            TaskViewer.selected_nodes = [TaskViewer.nodes[i] for i in new]

        s1.selected.on_change('indices', select_callback)


        remove_button = Button(label='Remove Selected')

        def remove_callback(event):
            target_list = s1.selected.indices
            for t in target_list:
                print('removing', t)
                TaskViewer.nodes[t].remove()

        remove_button.on_event(ButtonClick, remove_callback)

        run_button = Button(label='Run Selected')

        def run_callback(event):
            target_list = s1.selected.indices
            for t in target_list:
                print('running', t)
                TaskViewer.nodes[t].run()

        run_button.on_event(ButtonClick, run_callback)

        removeupstream_button = Button(label='Remove Upstream')

        def removeupstream_callback(event):
            def _removeupstream(t, dag):
                TaskViewer.nodes[t].remove()
                for ti in dag[t]:
                    _removeupstream(ti, dag)

            dag = self.dag
            dag.update({0: {}})
            target_list = s1.selected.indices
            for t in target_list:
                _removeupstream(t, dag)
            return

        removeupstream_button.on_event(ButtonClick, removeupstream_callback)

        placeroute_button = Button(label='Place and Route')

        def placeroute_callback(event):
            step = 0.1

            def get_adjacent_nodes():
                ac_g = self.rev_dag
                ac_g = {k: set(v) for k, v in ac_g.items()}
                for k, v in ac_g.items():
                    for i in v:
                        ac_g[i].add(k)

                ac_g = {k: list(v) for k, v in ac_g.items()}
                return ac_g

            def attracting_xy(x, y):
                dx = np.subtract.outer(x, x)
                dy = np.subtract.outer(y, y)
                dxy_norm = np.sqrt(dx ** 2 + dy ** 2)

                #                 fx=np.sum(np.nan_to_num( (dx/dxy_norm) / (dxy_norm+1e-3)**2 ),axis=0)
                #                 fy=np.sum(np.nan_to_num( (dy/dxy_norm) / (dxy_norm+1e-3)**2 ),axis=0)
                fx = np.sum(np.nan_to_num(dx / dxy_norm), axis=0)
                fy = np.sum(np.nan_to_num(dy / dxy_norm), axis=0)

                return fx, fy

            def repelling_xy(x, y):
                dx = np.subtract.outer(x, x)
                dy = np.subtract.outer(y, y)
                dxy_norm = np.sqrt(dx ** 2 + dy ** 2)

                fx = -np.sum(np.nan_to_num((dx / dxy_norm) / (dxy_norm + 1) ** 2), axis=0)
                fy = -np.sum(np.nan_to_num((dy / dxy_norm) / (dxy_norm + 1) ** 2), axis=0)
                return fx, 5 * fy

            y = np.asarray(s1.data['y'])
            x = np.asarray(s1.data['x'])
            fy = np.zeros(y.shape)
            fx = np.zeros(x.shape)

            fx, fy = attracting_xy(x, y)

            adj_nodes = get_adjacent_nodes()
            for k, v in adj_nodes.items():
                sel_ind = v
                _fx, _fy = repelling_xy(x[sel_ind], y[sel_ind])
                fx[sel_ind] += _fx
                fy[sel_ind] += _fy

            #             x= (x + fx*step)
            #             x= np.nan_to_num(x)
            y = (y + fy * step)
            y = np.nan_to_num(y)
            edges_x, edges_y = self.get_edges(x, y)
            s1.data.update(
                x=x,
                y=y,
                edges_x=edges_x,
                edges_y=edges_y,
            )

        placeroute_button.on_event(ButtonClick, placeroute_callback)

        load_button = Button(label='Load Selected')
        load_callback = CustomJS(args=dict(), code="""
        var nb = Jupyter.notebook
        nb.insert_cell_below()
        var cell = nb.select_next().get_selected_cell()
        cell.set_text("[n.load() for n in TaskViewer.selected_nodes]")
        cell.execute()
        """)
        load_button.js_on_event(ButtonClick, load_callback)

        update_button = Button(label='Update')

        def update_callback():
#             print("updating every %d ms" % (self.update_period*1000))
            complete = self.update_complete()
            s2.data = {k: [vi for i, vi in enumerate(v) if not complete[i]] for k, v in source_dict.items()}
            return

        doc.add_periodic_callback(update_callback, self.update_period*1000)

        update_button.on_event(ButtonClick, lambda event: update_callback)

        l = column(
            children=[
                plot,
                row(
                    load_button,
                    run_button,
                    remove_button,
                    removeupstream_button,
                    # update_button,
                    #                     placeroute_button,
                )
            ],
        )

        doc.add_root(l)

    def show(self, url="http://127.0.0.1", jupyter_notebook_port=8888, bokeh_port=5006):
#         from bokeh.io import output_notebook


#         output_notebook()
        notebook_url = ":".join([url, str(jupyter_notebook_port)])
        return show_app(self.plot_task, curstate(), notebook_url=notebook_url, port=bokeh_port)

