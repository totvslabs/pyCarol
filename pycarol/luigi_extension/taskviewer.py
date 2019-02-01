import numpy as np
import pandas as pd

class TaskViewer(object):
    nodes = []
    selected_nodes = []

    def __init__(self, task, parallel_update=True):
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
        family = [t.split('.')[-2] if '.' in t else "empty_namespace" for t in task_names]
        task_names = [t.split('.')[-1] for t in task_names]
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
            tasklog=self.tasklog,
        )
        return source_dict

    def update_complete(self):
        import concurrent
        if self.parallel_update:
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                complete = executor.map(lambda x: x.complete(), TaskViewer.nodes)
                tasklog = executor.map(lambda x: x.loadlog(), TaskViewer.nodes)
            self.complete = list(complete)
            self.tasklog = list(tasklog)

        else:
            self.complete = [t.complete() for t in TaskViewer.nodes ]
            self.tasklog = [t.loadlog()  for t in TaskViewer.nodes ]
        self.tasklog = [log if isinstance(log,str) else "Log type is wrong." for log in self.tasklog]
        return self.complete

    def get_colormapper(self):
        family = self.family
        factors = list(set(family))
        nb_factors = np.clip(len(factors),3,10)
        from bokeh.palettes import Category10
        from bokeh.models.mappers import CategoricalColorMapper
        return CategoricalColorMapper(factors=factors, palette=Category10[nb_factors])

    def plot_task(self, doc,web_app=False):
        # plot dashboard
        print('start plot_task')
        doc.clear()
        from bokeh.models import Circle, Text, Button
        from bokeh.models import ColumnDataSource, CDSView, BooleanFilter, HoverTool
        from bokeh.plotting import figure
        from bokeh.layouts import column, row, layout
        from bokeh.transform import transform

        source_dict = self.get_source_dict()
        cm = self.get_colormapper()

        TOOLTIPS = [
            ('index', "$index"),
            ('task_id', '@task_id'),
        ]
        hover = HoverTool(names=['alltasks'])
        plot = figure(
            title="Pipeline Debugger",
            x_range=(-1, max(source_dict['x']) + 1),
            y_range=(-1, max(source_dict['y']) + 1),
            tools=[hover, 'wheel_zoom', 'pan', 'tap', 'box_select', 'reset'],
            toolbar_location='right',
            tooltips=TOOLTIPS,
            plot_width=900,
            plot_height=500,
        )

        s1 = ColumnDataSource(data=source_dict)
        s1_edges = ColumnDataSource(data={k:v for k,v in source_dict.items() if 'edges' in k})
        complete = self.update_complete()
#         print(complete)
        s2 = ColumnDataSource(data={k: [vi for i, vi in enumerate(v) if not complete[i]] for k, v in
                                    source_dict.items()})  # select only the rows of source_dict where complete==False
        done_tasks = CDSView(source=s1, filters=[BooleanFilter(complete)])
        notdone_tasks = CDSView(source=s1, filters=[BooleanFilter([not c for c in complete])])

        ml = plot.multi_line(xs='edges_x', ys='edges_y', source=s1_edges, color='gray',line_alpha=0.3)
        ml.nonselection_glyph = None
        plot.circle('x', 'y', source=s1, size=20, name='alltasks', color=transform('family', cm), legend='family')
        plot.circle('x', 'y', source=s2, size=10, color='white')
        plot.text(x='x', y='y', text='task_names', source=s1, text_font_size='8pt')

        from bokeh.events import ButtonClick
        from bokeh.models import Button
        from bokeh.layouts import widgetbox
        from bokeh.models.widgets import PreText
        pre = PreText(text="")

        def select_callback(attr, old, new):
            TaskViewer.selected_nodes = [TaskViewer.nodes[i] for i in new]
            pre.text = self.tasklog[new[0]]
            print("selected {}".format(new))

        s1.selected.on_change('indices', select_callback)


        remove_button = Button(label='Remove Selected')

        def remove_callback(event):
            target_list = s1.selected.indices
            for t in target_list:
                print('removing', t)
                TaskViewer.nodes[t].remove()

        remove_button.on_event(ButtonClick, remove_callback)


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


        update_button = Button(label='Update')

        def update_callback(event):
            complete = self.update_complete()
            s2.data = {k: [vi for i, vi in enumerate(v) if not complete[i]] for k, v in source_dict.items()}
            return

        update_button.on_event(ButtonClick, update_callback)

        if web_app:
            l = column(
                [
                    plot,
                    row(
                        remove_button,
                        removeupstream_button,
                        update_button,
                    ),
                    widgetbox(pre),
                ],
                sizing_mode='scale_width'
            )
        else:
            l = column(
                children=[
                    plot,
                    row(
                        remove_button,
                        removeupstream_button,
                        update_button,
                    )
                ],
            )

        doc.add_root(l)

    def show(self, url="http://127.0.0.1", jupyter_notebook_port=8888, bokeh_port=5006):
        from bokeh.io.state import curstate
        from bokeh.io.notebook import show_app
        from bokeh.io import output_notebook

        output_notebook()

        notebook_url = ":".join([url, str(jupyter_notebook_port)])
        return show_app(self.plot_task, curstate(), notebook_url=notebook_url, port=bokeh_port)

