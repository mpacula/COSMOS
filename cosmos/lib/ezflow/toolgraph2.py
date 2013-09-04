import itertools as it
import re
from collections import namedtuple

import networkx as nx
import pygraphviz as pgv

from cosmos.utils.helpers import groupby, get_all_dependencies
from cosmos.Workflow.models import Task, TaskError
from cosmos.lib.ezflow.tool import Tool, INPUT


class DAGError(Exception): pass
class StageNameCollision(Exception):pass
class FlowFxnValidationError(Exception):pass
class ShardError(Exception):pass

def list_optional(variable, klass):
    if isinstance(variable,list) and isinstance(variable[0],klass):
        return variable
    elif isinstance(variable,klass):
        return [variable]
    else:
        raise TypeError, 'variable must be a list of {0} or a {0}'.format(klass)

class ToolGraph(object):
    """
    A Representation of a workflow as a :term:`ToolGraph` of jobs.
    """
    
    def __init__(self,cpu_req_override=False,mem_req_factor=1):
        """
        :param cpu_req_override: set to an integer to override all task cpu_requirements.  Useful when a :term:`DRM` does not support requesting multiple cpus
        :param mem_req_factor: multiply all task mem_reqs by this number.
        :param dag.ignore_stage_name_collisions:  Allows the flowfxns to add to stages that already exists.
        """
        self.G = nx.DiGraph()
        self.Stage_G = nx.DiGraph()
        self.cpu_req_override = cpu_req_override
        self.mem_req_factor = mem_req_factor

    def stage(self, tool=None, parents=None, shard=None, name=None, extra_tags=None):
        """
        Creates a Stage in this TaskGraph
        """
        stage = Stage(tool, parents, shard, name, extra_tags)
        assert stage.name not in [n.name for n in self.Stage_G.nodes()], 'Duplicate stage names detected: {0}'.format(stage.name)

        self.Stage_G.add_node(stage)
        for parent in stage.parents:
            self.Stage_G.add_edge(parent, stage)

        return stage

    def resolve(self,settings,parameters):
        self._resolve_tools()
        self.configure(settings,parameters)
        return self

    # def resolve(self, stages, settings, parameters):
    #     stages = list_optional(stages,Stage)
    #
    #     def recurse(u, v):
    #         if not self.Stage_G.has_edge(u, v):
    #             self.Stage_G.add_edge(u, v)
    #             for p in u.parents:
    #                 recurse(p, u)
    #     for s in stages:
    #         for p in s.parents:
    #             recurse(p, s)
    #
    #     stage_names_used = [ s.name for s in self.Stage_G.nodes() ]
    #     assert len(set(stage_names_used)) == len(stage_names_used), 'Duplicate stage names detected'
    #
    #     self._resolve_tools()
    #     self.configure(settings,parameters)
    #     return self

    def add_tool_to_G(self,new_tool,parents=None):
        if parents is None:
            parents = []
        assert new_tool.tags not in [ t.tags for t in self.G.nodes() if t.stage == new_tool.stage ], 'Duplicate set of tags detected in {0}'.format(new_tool.stage)

        self.G.add_node(new_tool)
        for p in parents:
            self.G.add_edge(p,new_tool)

    def _resolve_tools(self):
        for stage in nx.topological_sort(self.Stage_G):
            if issubclass(stage.tool,INPUT):
                #stage.tools is already set
                for tool in stage.tools:
                    self.add_tool_to_G(tool)

            elif isinstance(stage.shard,one2one):
                for parent_tool in it.chain(*[ s.tools for s in stage.parents ]):
                    tags2 = parent_tool.tags.copy()
                    tags2.update(stage.extra_tags)
                    new_tool = stage.tool(stage=stage,dag=self,tags=tags2)
                    stage.tools.append(new_tool)
                    self.add_tool_to_G(new_tool,[parent_tool])

            elif isinstance(stage.shard, many2one):
                keywords = stage.shard.keywords
                if type(keywords) != list:
                    raise TypeError('keywords must be a list')
                if any(k == '' for k in keywords):
                    raise TypeError('keyword cannot be an empty string')

                parent_tools = list(it.chain(*[ s.tools for s in stage.parents ]))
                parent_tools_without_all_keywords = filter(lambda t: not all([k in t.tags for k in keywords]), parent_tools)
                parent_tools_with_all_keywords = filter(lambda t: all(k in t.tags for k in keywords), parent_tools)

                if len(parent_tools_with_all_keywords) == 0: raise ShardError, 'Parent stages must have at least one tool with all many2one keywords of {0}'.format(keywords)

                for tags, parent_tool_group in groupby(parent_tools_with_all_keywords, lambda t: dict((k,t.tags[k]) for k in keywords if k in t.tags)):
                    parent_tool_group = list(parent_tool_group) + parent_tools_without_all_keywords
                    tags.update(stage.extra_tags)
                    new_tool = stage.tool(stage=stage,dag=self,tags=tags)
                    stage.tools.append(new_tool)
                    self.add_tool_to_G(new_tool,parent_tool_group)

            elif isinstance(stage.shard, one2many):
                parent_tools = list(it.chain(*[ s.tools for s in stage.parents ]))
                #: splits = [[(key1,val1),(key1,val2),(key1,val3)],[(key2,val1),(key2,val2),(key2,val3)],[...]]
                splits = [ list(it.product([split[0]],split[1])) for split in stage.shard.split_by ]
                for parent_tool in parent_tools:
                    for new_tags in it.product(*splits):
                        tags = dict(parent_tool.tags).copy()
                        tags.update(stage.extra_tags)
                        tags.update(dict(new_tags))
                        new_tool = stage.tool(stage=stage,dag=self,tags=tags)
                        stage.tools.append(new_tool)
                        self.add_tool_to_G(new_tool,[parent_tool])

            else:
                raise AssertionError, 'Stage constructed improperly'
        return self

    def as_image(self,resolution='stage',save_to=None):
        """
        Writes the :term:`ToolGraph` as an image.
        gat
        :param path: the path to write to
        """
        dag = pgv.AGraph(strict=False, directed=True, fontname="Courier", fontsize=11)
        dag.node_attr['fontname']="Courier"
        dag.node_attr['fontsize']=8
        dag.graph_attr['fontsize']=8
        dag.edge_attr['fontcolor']='#586e75'
        #dag.node_attr['fontcolor']='#586e75'
        dag.graph_attr['bgcolor']='#fdf6e3'


        if resolution=='stage':
            dag.add_nodes_from([ n.label for n in self.Stage_G.nodes() ])
            for u,v,attr in self.Stage_G.edges(data=True):
                if isinstance(v.shard,many2one):
                    dag.add_edge(u.label,v.label,label=v.shard,style='dotted',arrowhead='odiamond')
                elif isinstance(v.shard,one2many):
                    dag.add_edge(u.label,v.label,label=v.shard,style='dashed',arrowhead='crow')
                else:
                    dag.add_edge(u.label,v.label,label=v.shard,arrowhead='vee')
        elif resolution=='tool':
            dag.add_nodes_from(self.G.nodes())
            dag.add_edges_from(self.G.edges())
            for stage,tools in groupby(self.G.nodes(),lambda x:x.stage):
                sg = dag.add_subgraph(name="cluster_{0}".format(stage),label=stage.label,color='lightgrey')
        else:
            raise TypeError, '`type` must be `stage` or `tool'

        dag.layout(prog="dot")
        return dag.draw(path=save_to,format='svg')

    def configure(self,settings={},parameters={}):
        """
        Sets the parameters an settings of every tool in the dag.
        
        :param parameters: (dict) {'stage_name': { 'name':'value', ... }, {'stage_name2': { 'key':'value', ... } }
        :param settings: (dict) { 'key':'val'} }
        """
        self.parameters = parameters
        for tool in self.G.node:
            tool.settings = settings
            if tool.stage.name not in self.parameters:
                #set defaults, then override with parameters
                self.parameters[tool.stage.name] = tool.default_params.copy()
                self.parameters[tool.stage.name].update(parameters.get(tool.__class__.__name__,{}))
                self.parameters[tool.stage.name].update(parameters.get(tool.stage.name,{}))
            tool.parameters = self.parameters.get(tool.stage.name,{})
        return self
            
    def add_to_workflow(self,workflow):
        """
        Add this dag to a workflow.  Only adds tools to stages that are new, that is, another tag in the same
        stage with the same tags does not already exist.

        :param workflow: the workflow to add to
        """
        workflow.log.info('Adding tasks to workflow.')
        
        ### Validation
        taskfiles = list(it.chain(*[ n.output_files for n in self.G.nodes() ]))

        # check paths for duplicates.
        # TODO this should also be done at the DB level since resuming can add taskfiles with the same path
        paths = [ tf.path for tf in taskfiles if tf.path ]
        if len(paths) != len(set(paths)):
            import pprint
            raise DAGError('Multiple taskfiles refer to the same path.  Paths should be unique. taskfile.paths are:{0}'.format(pprint.pformat(sorted(paths))))

        ### Add stages, and set the tool.stage reference for all tools

        # Load stages or add if they don't exist
        django_stages = {}
        for stage in nx.topological_sort(self.Stage_G):
            django_stages[stage.name] = workflow.add_stage(stage.name)

        # Get the tasks associated with the tools that already successful
        stasks = list(workflow.tasks.select_related('_output_files','stage'))


        ### Validation
        should_be_unique = [ (tool.stage, tuple(tool.tags.items())) for tool in self.G.nodes() ]
        assert len(should_be_unique) == len(set(should_be_unique)), 'A stage has tools with duplicate tags'


        # Delete tasks who's dependencies have changed
        successful_already = intersect_tool_task_graphs(self.G.nodes(), stasks)
        for tool, task in successful_already:
            tool._successful_task = task

        # TODO delete tasks that are in DB but not in toolgraph

        changed_dependencies = get_all_dependencies(self.G, map(lambda x: x[0], successful_already), include_source=False)
        delete_tasks = [ tool._successful_task for tool in changed_dependencies if hasattr(tool,'_successful_task') ]
        if len(delete_tasks):
            workflow.bulk_delete_tasks(delete_tasks)

        # Update successful tools that are still in the database
        for tool in [ tool for tool, task in successful_already if task not in changed_dependencies ]:
            tool.output_files = tool._successful_task.output_files
            tool._task_instance = tool._successful_task

        #bulk save tasks
        new_nodes = filter(lambda n: not hasattr(n,'_task_instance'), nx.topological_sort(self.G))
        workflow.log.info('Total tasks: {0}, New tasks being added: {1}'.format(len(self.G.nodes()),len(new_nodes)))

        #bulk save task_files.  All inputs have to at some point be an output, so just bulk save the outputs.
        #Must come before adding tasks, since taskfile.ids must be populated to compute the proper pcmd.
        taskfiles = list(it.chain(*[ n.output_files for n in new_nodes ]))
        workflow.bulk_save_taskfiles(taskfiles)
        
        #bulk save tasks
        for node in new_nodes:
            node._task_instance = self.__new_task(django_stages[node.stage.name],node)
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
        workflow.stage_graph = self.as_image(resolution='stage')
        workflow.save()

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
                      memory_requirement = tool.mem_req * self.mem_req_factor,
                      cpu_requirement = tool.cpu_req if not self.cpu_req_override else self.cpu_req_override,
                      time_requirement = tool.time_req,
                      NOOP = tool.NOOP,
                      succeed_on_failure = tool.succeed_on_failure)
        except TaskError as e:
            raise TaskError('{0}. Task is {1}.'.format(e,tool))



class Shard(object):
    """Abstract Class for the various shard strategies"""
    def __init__(self,*args,**kwargs):
        self.args = args
        self.kwargs = kwargs
    def __str__(self):
        m = re.search("^(\w).+2(\w).+$",type(self).__name__)
        return '{0}2{1}'.format(m.group(1),m.group(2))

class one2one(Shard):
    pass

class many2one(Shard):
    def __init__(self,keywords,*args,**kwargs):
        assert isinstance(keywords, list), '`keywords` must be a list'
        self.keywords = keywords
        super(Shard,self).__init__(*args,**kwargs)
class one2many(Shard):
    def __init__(self,split_by,*args,**kwargs):
        assert isinstance(split_by, list), '`split_by` must be a list'
        if len(split_by) > 0:
            assert isinstance(split_by[0],tuple), '`split_by` must be a list of tuples'
            assert isinstance(split_by[0][0],str), 'the first element of tuples in `split_by` must be a str'
            assert isinstance(split_by[0][1],list), 'the second element of the tuples in the `split_by` list must also be a list'

        self.split_by = split_by
        super(Shard,self).__init__(*args,**kwargs)

class Stage():
    def __init__(self, tool=None, parents=None, shard=None, name=None, extra_tags=None):
        if parents is None:
            parents = []
        if extra_tags is None:
            extra_tags = {}
        if shard == one2one or shard is None:
            shard = one2one()

        if isinstance(tool,type):
            # Stage initialized normally
            assert issubclass(tool, Tool), '`tool` must be a subclass of `Tool`'
            assert isinstance(shard,Shard), '`shard` must be of type `Shard`'

            self.tool = tool
            self.tools = []
            self.parents = list_optional(parents, Stage)
            self.shard = shard

        elif isinstance(tool,list):
            # Stage initialized from a set of INPUTs
            assert isinstance(tool[0], INPUT), '`tools` must be a list of `INPUT`s'
            tags = [ tuple(t.tags.items()) for t in tool]
            assert len(tags) == len(set(tags)), 'Duplicate tool tags detected for {0}.  Tags within a stage must be unique.'.format(INPUT)
            self.tool = INPUT
            self.tools = tool
            self.parents = []
            self.shard = None

            for tool in self.tools:
                tool.stage = self
        else:
            raise TypeError, 'Incorrect parameter types'

        self.extra_tags = extra_tags
        self.name = name or self.tool.__name__


    @property
    def label(self):
        return '{0} (x{1})'.format(self.name,len(self.tools))

    def __str__(self):
        return '<Stage {0}>'.format(self.name)


def intersect_tool_task_graphs(tools,tasks):
    intersection = []
    for tpl, group in groupby(tools + tasks, lambda x: (x.tags, x.stage.name)):
        group = list(group)
        assert len(group) < 3, 'Database integrity error'
        if len(group) == 2:
            tool = group[0] if isinstance(group[1],Task) else group[1]
            task = group[0] if isinstance(group[0],Task) else group[1]
            intersection.append((tool,task))
    return intersection
