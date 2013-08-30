import itertools as it
import re

import networkx as nx
import pygraphviz as pgv
from decorator import decorator

from cosmos.utils.helpers import groupby
from cosmos.Workflow.models import Task,TaskError
from cosmos.lib.ezflow2.tool import Tool,INPUT


class DAGError(Exception): pass
class StageNameCollision(Exception):pass
class FlowFxnValidationError(Exception):pass


def list_optional(variable, list_type):
    if isinstance(variable,list) and isinstance(variable[0],Shard):
        return variable
    elif isinstance(variable,Shard):
        return [variable]
    else:
        raise TypeError, 'variable must be a list of {0} or a {0}'.format(list_type)


class Shard(object):
    def __init__(self,*args,**kwargs):
        self.args = args
        self.kwargs = kwargs

class one2one(Shard): pass
class many2one(Shard): pass

class Stage():
    def __init__(self, tool, depends_on=None, shard=None):
        if depends_on is None:
            depends_on = []
        assert shard is None or isinstance(shard,Shard), '`shard` must be a subclass of `Shard`'
        if isinstance(tool, type) and issubclass(tool, Tool):
            self.tool = tool
            self.parents = list_optional(depends_on, Stage)
            self.shard = shard
        elif isinstance(tool, list) and isinstance(tool[0], INPUT):
            self.tool = INPUT
            self.inputs = tool
        else:
            raise TypeError, '`tool` must be of type `Tool` or a list of `INPUT`s'

class ToolGraph(object):
    """
    A Representation of a workflow as a :term:`ToolGraph` of jobs.
    """
    
    def __init__(self,cpu_req_override=False,ignore_stage_name_collisions=False,mem_req_factor=1):
        """
        :param cpu_req_override: set to an integer to override all task cpu_requirements.  Useful when a :term:`DRM` does not support requesting multiple cpus
        :param mem_req_factor: multiply all task mem_reqs by this number.
        :param dag.ignore_stage_name_collisions:  Allows the flowfxns to add to stages that already exists.
        """
        self.G = nx.DiGraph()
        self.Stage_G = nx.DiGraph()
        self.cpu_req_override = cpu_req_override
        self.mem_req_factor = mem_req_factor
        #self.stage_names_used = []
        self.ignore_stage_name_collisions = ignore_stage_name_collisions

    def add_recursively(self, stage):
        if isinstance(stage,Tool):
            stage = [stage]
        assert isinstance(stage,list) and isinstance(stage[0],Tool), '`using` must be a Tool or a list of Tools'

        def recurse(u,v):
            if not self.Stage_G.has_edge(u,v):
                self.Stage_G.add_edge(u,v)
                for p in u.parents:
                    recurse(p,v)
        for p in stage.parents:
            recurse(p,stage)

    def create_stage_img(self,path):
        """
        Writes the :term:`ToolGraph` as an image.
        gat
        :param path: the path to write to
        """
        dag = pgv.AGraph(strict=False, directed=True, fontname="Courier", fontsize=11)
        dag.node_attr['fontname']="Courier"
        dag.node_attr['fontsize']=8
        dag.add_edges_from(self.G.edges())
        dag.layout(prog="dot")
        dag.draw(path,format='svg')
        print 'wrote to {0}'.format(path)
        return self


    def create_dag_img(self,path):
        """
        Writes the :term:`ToolGraph` as an image.
        gat
        :param path: the path to write to
        """
        dag = pgv.AGraph(strict=False,directed=True,fontname="Courier",fontsize=11)
        dag.node_attr['fontname']="Courier"
        dag.node_attr['fontsize']=8
        dag.add_edges_from(self.G.edges())
        for stage,tasks in groupby(self.G.nodes(),lambda x:x.stage_name):
            sg = dag.add_subgraph(name="cluster_{0}".format(stage),label=stage,color='lightgrey')
            for task in tasks:
                sg.add_node(task,label=task.label)    
        
        dag.layout(prog="dot")
        dag.draw(path,format='svg')
        print 'wrote to {0}'.format(path)
        return self

    def configure(self,settings={},parameters={}):
        """
        Sets the parameters an settings of every tool in the dag.
        
        :param parameters: (dict) {'stage_name': { 'name':'value', ... }, {'stage_name2': { 'key':'value', ... } }
        :param settings: (dict) { 'key':'val'} }
        """
        self.parameters = parameters
        for tool in self.G.node:
            tool.settings = settings
            if tool.stage_name not in self.parameters:
                #set defaults, then override with parameters
                self.parameters[tool.stage_name] = tool.default_params.copy()
                self.parameters[tool.stage_name].update(parameters.get(tool.__class__.__name__,{}))
                self.parameters[tool.stage_name].update(parameters.get(tool.stage_name,{}))
            tool.parameters = self.parameters.get(tool.stage_name,{})
        return self
            
    def add_to_workflow(self,workflow):
        """
        Add this dag to a workflow.  Only adds tools to stages that are new, that is, another tag in the same
        stage with the same tags does not already exist.

        :param workflow: the workflow to add
        """
        workflow.log.info('Adding tasks to workflow.')
        
        #Validation
        taskfiles = list(it.chain(*[ n.output_files for n in self.G.nodes() ]))
        #check paths
        #TODO this code is really weird.
        v = map(lambda tf: tf.path,taskfiles)
        v = filter(lambda x:x,v)
        if len(map(lambda t: t,v)) != len(map(lambda t: t,set(v))):
            import pprint
            raise DAGError('Multiple taskfiles refer to the same path.  Paths should be unique. taskfile.paths are:{0}'.format(pprint.pformat(sorted(v))))

        #Add stages, and set the tool.stage reference for all tools
        stages = {}
        # for tool in nx.topological_sort(self.G):
        #     stage_name = tool.stage_name
        #     if stage_name not in stages: #have not seen this stage yet
        #         stages[stage_name] = workflow.add_stage(stage_name)
        #     tool.stage = stages[stage_name]

        # Load stages or add if they don't exist
        for stage_name in self.stage_names_used:
            stages[stage_name] = workflow.add_stage(stage_name)

        # Set tool.stage
        for tool in self.G.nodes():
            tool.stage = stages[tool.stage_name]

        #update tool._task_instance and tool.output_files with existing data
        stasks = list(workflow.tasks.select_related('_output_files','stage'))
        for tpl, group in groupby(stasks + self.G.nodes(), lambda x: (x.tags,x.stage.name)):
            group = list(group)
            if len(group) >1:
                tags = tpl[0]
                stage_name = tpl[1]
                tool = group[0] if isinstance(group[1],Task) else group[1]
                task = group[0] if isinstance(group[0],Task) else group[1]
                tool.output_files = task.output_files
                tool._task_instance = task
        
        #bulk save tasks
        new_nodes = filter(lambda n: not hasattr(n,'_task_instance'), nx.topological_sort(self.G))
        workflow.log.info('Total tasks: {0}, New tasks being added: {1}'.format(len(self.G.nodes()),len(new_nodes)))
        
        #bulk save task_files.  All inputs have to at some point be an output, so just bulk save the outputs.
        #Must come before adding tasks, since taskfile.ids must be populated to compute the proper pcmd.
        taskfiles = list(it.chain(*[ n.output_files for n in new_nodes ]))
        workflow.bulk_save_taskfiles(taskfiles)
        
        #bulk save tasks
        for node in new_nodes:
                node._task_instance = self.__new_task(node.stage,node)
        tasks = [ node._task_instance for node in new_nodes ]
        workflow.bulk_save_tasks(tasks)
        
        ### Bulk add task->output_taskfile relationships
        ThroughModel = Task._output_files.through
        rels = [ ThroughModel(task_id=n._task_instance.id,taskfile_id=tf.id) for n in new_nodes for tf in n.output_files ]
        ThroughModel.objects.bulk_create(rels)

        ### Bulk add task->input_taskfile relationships
        ThroughModel = Task._input_files.through
        rels = [ ThroughModel(task_id=n._task_instance.id,taskfile_id=tf.id) for n in new_nodes for tf in n.input_files ]

        ThroughModel.objects.bulk_create(rels)


        ### Bulk add task->parent_task relationships
        ThroughModel = Task._parents.through
        new_edges = filter(lambda e: e[0] in new_nodes or e[1] in new_nodes,self.G.edges())
        rels = [ ThroughModel(from_task_id=child._task_instance.id,
                              to_task_id=parent._task_instance.id)
                 for parent,child in new_edges ]
        ThroughModel.objects.bulk_create(rels)


        #bulk save edges
        new_edges = filter(lambda e: e[0] in new_nodes or e[1] in new_nodes,self.G.edges())
        task_edges = [ (parent._task_instance,child._task_instance) for parent,child in new_edges ]
        workflow.bulk_save_task_edges(task_edges)

    def add_run(self,workflow,finish=True):
        """
        Shortcut to add to workflow and then run the workflow
        :param workflow: the workflow this dag will be added to
        :param finish: pass to workflow.run()
        """
        self.add_to_workflow(workflow)
        workflow.run(finish=finish)


    def __new_task(self,stage,tool):
        """
        Instantiates a task from a tool.  Assumes TaskFiles already have real primary keys.

        :param stage: The stage the task should belong to.
        :param tool: The Tool.
        """
        pcmd = tool.pcmd
        # for m in re.findall('(#F\[(.+?):(.+?):(.+?)\])',pcmd):
        #     if m[1] not in [t.id for t in tool.output_files]:
        #         tool.input_files.append(TaskFile.objects.get(pk=m[1]))

        try:
            return Task(
                      stage = stage,
                      pcmd = pcmd,
                      tags = tool.tags,
                      # input_files = tool.input_files,
                      # output_files = tool.output_files,
                      memory_requirement = tool.mem_req * self.mem_req_factor,
                      cpu_requirement = tool.cpu_req if not self.cpu_req_override else self.cpu_req_override,
                      time_requirement = tool.time_req,
                      NOOP = tool.NOOP,
                      succeed_on_failure = tool.succeed_on_failure)
        except TaskError as e:
            raise TaskError('{0}. Task is {1}.'.format(e,tool))