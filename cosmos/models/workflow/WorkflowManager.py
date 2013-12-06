from django.db import models
from ...utils import helpers

class WorkflowManager():
    def __init__(self, workflow):
        self.workflow = workflow
        self.dag = self.createDiGraph()
        self.workflow.log.info('Using TaskGraph to create Job Queue')
        self.dag_queue = self.dag.copy()
        self.dag_queue.remove_nodes_from(map(lambda x: x['id'], workflow.tasks.filter(successful=True).values('id')))
        self.queued_tasks = []

    def queue_task(self, task):
        self.queued_tasks.append(task.id)

    def queue_is_empty(self):
        return len(self.dag_queue.nodes()) == 0

    # def run_ready_tasks(self):
    #     ready_tasks = [ n for n in self.get_ready_tasks() ]
    #
    #     for n in ready_tasks:
    #         self.queue_task(n)
    #         self.workflow._run_task(n)
    #
    #     return ready_tasks

    def complete_task(self, task):
        """
        Run everytime a task completes so that the dag can stay up to date
        """
        self.dag_queue.remove_node(task.id)
        self.dag.node[task.id]['status'] = task.status

        if task.status == 'successful' and self.workflow.delete_intermediates:
            # Input files may be ready for intermediate deleting if all
            # Tasks that depend on them are also successful, and they are not an input file (ie has >0 parents)
            for infile in task.input_files:
                if not infile.persist:
                    if all([r['successful'] and r['_parents__count'] > 0
                            for r in
                            infile.task_input_set.values('successful', 'id').annotate(models.Count('_parents'))]
                    ):
                        infile.delete_because_intermediate()

        return self


    # def is_task_intermediate(self,task_id):
    #     """
    #     Checks to see if a task_id is an intermediary task.
    #     An intermediary task has at least 1 child and 1 parent, and all of its children are all successful.
    #     """
    #     successors = self.dag.successors(task_id)
    #     if len(self.dag.predecessors(task_id)) > 0 and len(successors) > 0:
    #         return all( self.dag.node[s]['status'] == 'successful' for s in successors )

    def get_ready_tasks(self):
        degree_0_tasks = map(lambda x: x[0], filter(lambda x: x[1] == 0, self.dag_queue.in_degree().items()))
        return list(self.workflow.tasks.filter(id__in=filter(lambda x: x not in self.queued_tasks, degree_0_tasks)))
        #return map(lambda n_id: Task.objects.get(pk=n_id),filter(lambda x: x not in self.queued_tasks,degree_0_tasks)) 

    def createDiGraph(self):
        import networkx as nx

        dag = nx.DiGraph()
        dag.add_edges_from([(e['to_task'], e['from_task'])
                            for e in self.workflow.task_edges.values('to_task', 'from_task')])
        for stage in self.workflow.stages:
            stage_name = stage.name
            for task in stage.tasks.all():
                dag.add_node(task.id, tags=task.tags, status=task.status, stage=stage_name, url=task.url())
        return dag

    def createAGraph(self):
        import pygraphviz as pgv
        dag = pgv.AGraph(strict=False, directed=True, fontname="Courier")
        dag.node_attr['fontname'] = "Courier"
        dag.node_attr['fontcolor'] = '#586e75'
        dag.node_attr['fontsize'] = 8
        dag.graph_attr['fontsize'] = 8
        dag.edge_attr['fontcolor'] = '#586e75'
        dag.graph_attr['bgcolor'] = '#fdf6e3'

        dag.add_edges_from(self.dag.edges())
        for stage, tasks in helpers.groupby(self.dag.nodes(data=True), lambda x: x[1]['stage']):
            sg = dag.add_subgraph(name="cluster_{0}".format(stage), label=str(stage), color='#b58900')
            for n, attrs in tasks:
                def truncate_val(kv):
                    v = "{0}".format(kv[1])
                    v = v if len(v) < 10 else v[1:8] + '..'
                    return "{0}: {1}".format(kv[0], v)

                label = " \\n".join(map(truncate_val, attrs['tags'].items()))
                status2color = {'no_attempt': 'black', 'in_progress': 'gold1', 'successful': 'darkgreen',
                                'failed': 'darkred'}
                sg.add_node(n, label=label, URL=attrs['url'].format(n), target="_blank",
                            color=status2color[attrs['status']])

        return dag


    def as_img(self, format="svg"):
        g = self.createAGraph()
        #g = self.createAGraph(self.get_simple_dag())
        #g=nx.to_agraph(self.get_simple_dag())
        g.layout(prog="dot")
        return g.draw(format=format)

    def __str__(self):
        g = self.createAGraph()
        #g = self.createAGraph(self.get_simple_dag())
        #g=nx.to_agraph(self.get_simple_dag())
        return g.to_string()