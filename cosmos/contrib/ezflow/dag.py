from cosmos.utils.helpers import groupby
import itertools as it
import networkx as nx
import pygraphviz as pgv
from cosmos.Workflow.models import Task,TaskError
from decorator import decorator

class DAGError(Exception): pass

class DAG(object):
    """
    A Representation of a workflow as a :term:`DAG` of jobs.
    """
    
    def __init__(self,cpu_req_override=False,mem_req_factor=1):
        """
        :param cpu_req_override: set to an integer to override all task cpu_requirements.  Useful when a :term:`DRM` does not support requesting multiple cpus
        :param mem_req_factor: multiply all task mem_reqs by this number.
        """
        self.G = nx.DiGraph()
        self.last_tools = []
        self.cpu_req_override = cpu_req_override
        self.mem_req_factor = mem_req_factor
        self.stage_names_used = []

    def get_tasks_by(self,stage_names=[],tags={}):
        """
        :param stage_names: (str) Only returns tasks belonging to stages in stage_names
        :param tags: (dict) The criteria used to decide which tasks to return.
        :return: (list) A list of tasks
        """
        for stage_name in stage_names:
            if stage_name not in self.stage_names_used:
                raise KeyError, 'Stage name "{0}" does not exist.'.format(stage_name)

        def dict_intersection_is_equal(d1,d2):
            for k,v in d2.items():
                try:
                    if d1[k] != v:
                        return False
                except KeyError:
                    pass
            return True

        tasks = [ task for task in self.G.nodes()
                  if (task.stage_name in stage_names)
            and dict_intersection_is_equal(task.tags,tags)
        ]
        return tasks

    def branch(self,stage_names=[],tags={}):
        """
        Updates last_tools to be the tools in the stages with name stage_name.
        The next infix operation will thus be applied to `stage_name`.
        This way the infix operations an be applied to multiple stages if the workflow isn't "linear".

        :param stage_names: (str) Only returns tasks belonging to stages in stage_names
        :param tags: (dict) The criteria used to decide which tasks to return.
        :return: (list) A list of tasks
        """
        self.last_tools = self.get_tasks_by(stage_names=stage_names,tags=tags)
        return self
        
    def create_dag_img(self,path):
        """
        Writes the :term:`DAG` as an image.
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
        Add this dag to a workflow.  Only adds tasks to stages that are new, that is, another tag in the same
        stage with the same tags does not already exist.

        :param workflow: the workflow to add
        """
        workflow.log.info('Adding tasks to workflow.')
        
        #Validation
        taskfiles = list(it.chain(*[ n.output_files for n in self.G.nodes() ]))
        #check paths
        v = map(lambda tf: tf.path,taskfiles)
        v = filter(lambda x:x,v)
        if len(map(lambda t: t,v)) != len(map(lambda t: t,set(v))):
            import pprint
            raise DAGError('Multiple taskfiles refer to the same path.  Paths should be unique. taskfile.paths are:{0}'.format(pprint.pformat(sorted(v))))

        #Add stages, and set the tool.stage reference for all tools
        stages = {}
        for tool in nx.topological_sort(self.G):
            stage_name = tool.stage_name
            if stage_name not in stages: #have not seen this stage yet
                stages[stage_name] = workflow.add_stage(stage_name)
            tool.stage = stages[stage_name]
        
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

        workflow.bulk_save_task_files(taskfiles)
        
        #bulk save tasks
        for node in new_nodes:
                node._task_instance = self.__new_task(node.stage,node)
        tasks = [ node._task_instance for node in new_nodes ]
        workflow.bulk_save_tasks(tasks)
        
        ### Bulk add task->output_taskfile relationships
        ThroughModel = Task._output_files.through
        rels = [ ThroughModel(task_id=n._task_instance.id,taskfile_id=out.id) for n in new_nodes for out in n.output_files ]
        ThroughModel.objects.bulk_create(rels)
        
        #bulk save edges
        new_edges = filter(lambda e: e[0] in new_nodes or e[1] in new_nodes,self.G.edges())
        task_edges = [ (parent._task_instance,child._task_instance) for parent,child in new_edges ]
        workflow.bulk_save_task_edges(task_edges)

    def add_run(self,workflow):
        """
        Shortcut to add to workflow and then run the workflow
        :param workflow:
        """
        self.add_to_workflow(workflow)
        workflow.run()


    def __new_task(self,stage,tool):
        """
        Instantiates a task from a tool.

        :param stage: The stage the task should belong to.
        :param tool: The Tool.
        """
        try:
            return Task(
                      stage = stage,
                      pcmd = tool.pcmd,
                      tags = tool.tags,
                      input_files = tool.input_files,
                      output_files = tool.output_files,
                      memory_requirement = tool.mem_req * self.mem_req_factor,
                      cpu_requirement = tool.cpu_req if not self.cpu_req_override else self.cpu_req_override,
                      time_requirement = tool.time_req,
                      NOOP = tool.NOOP,
                      dont_delete_output_files = tool.dont_delete_output_files,
                      succeed_on_failure = tool.succeed_on_failure)
        except TaskError as e:
            raise TaskError('{0}. Task is {1}.'.format(e,tool))
            


class dagError(Exception):pass

def f(LHS,params):
    pass

class Infix:
    def __init__(self, function):
        self.function = function
    def __ror__(self, other):
        return Infix(lambda x, self=self, other=other: self.function(other, *x if type(x) == tuple else (x,)))
    def __or__(self, other):
        return self.function(other)
    def __rlshift__(self, other):
        return Infix(lambda x, self=self, other=other: self.function(other, *x if type(x) == tuple else (x,)))
    def __rshift__(self, other):
        return self.function(other)
    def __call__(self, value1, value2):
        return self.function(value1, value2)
    
WF = None

@decorator
def flowfxn(func,dag,*RHS):
    """
    - The decorated function should return a generator, so evaluate it
    - Set the dag.last_tools to the decorated function's return value
    - Return the dag
    """
    if type(dag) != DAG: raise TypeError, 'The left hand side should be of type dag.DAG'
    dag.last_tools = list(func(dag,*RHS))
    try:
        stage_name = dag.last_tools[0].stage_name
    except IndexError:
        raise DAGError, 'Tried to apply |{0}| to DAG, but dag.last_tools is not set.  Make sure to |Add| some INPUTs first.'.format(
            func.__name__[1:].capitalize()
        )

    if stage_name in dag.stage_names_used: raise DAGError, 'Duplicate stage_names detected {0}.'.format(stage_name)
    dag.stage_names_used.append(stage_name)

    return dag

#Different from other flowfxns, so do not decorate with @flowfxn
#Experimental
# def _subworkflow(dag,subflow_class,parser=None):
#     """
#     Applies a :py:class:`flow.SubWorkflow` to the last tools added to the dag.
#     :param dag:
#     :param subflow_class: An instance which is a subclass of py:class:`flow.SubWorkflow`
#     :return: the new dag
#
#     >>> DAG() |Workflow| SubWorkflowClass
#     """
#     if type(dag) != DAG: raise TypeError, 'The left hand side should be of type dag.DAG'
#     subflow_class().flow(dag)
#     return dag
#
# SWF=Infix(_subworkflow)

@flowfxn
def _add(dag,tools,stage_name=None):
    """
    Always the first operator of a workflow.  Simply adds a list of tool instances to the dag, without adding any
    dependencies.

    .. warning::
        This operator is different than the others in that its input is a list of
        instantiated instances of Tools.

    :param dag: The dag to add to.
    :param tools: (list) Tool instances.
    :param stage_name: (str) The name of the stage to add to.  Defaults to the name of the tool class.
    :return: (list) The tools added.

    >>> dag() |Add| [tool1,tool2,tool3,tool4]
    """
    if stage_name is None:
        stage_name = tools[0].stage_name
    for tool in tools:
        tool.stage_name = stage_name
        dag.G.add_node(tool)
        yield tool
Add = Infix(_add)

@flowfxn
def _map(dag,tool_class,stage_name=None):
    """
    Creates a one2one relationships for each tool in the stage last added to the dag, with a new tool of
    type `tool_class`.

    :param dag: (dag) The dag to add to.
    :param parent_tools: (list) A list of parent tools.
    :param tool_class: (subclass of Tool)
    :param stage_name: (str) The name of the stage to add to.  Defaults to the name of the tool class.
    :return: (list) The tools added.

    >>> dag() |Map| Tool_Class
    """
    parent_tools = dag.last_tools
    for parent_tool in parent_tools:
        new_tool = tool_class(stage_name=stage_name,dag=dag,tags=parent_tool.tags)
        dag.G.add_edge(parent_tool,new_tool)
        yield new_tool
        
Map = Infix(_map)


#TODO raise exceptions if user submits bad kwargs for any infix commands
@flowfxn
def _split(dag,split_by,tool_class,stage_name=None):
    """
    Creates one2many relationships for each tool in the stage last added to the dag, with every possible combination
    of keywords in split_by.  New tools will be of class `tool_class` and tagged with one of the possible keyword
    combinations.

    :param dag: (dag) The dag to add to.
    :param parent_tools: (list) A list of parent tools.
    :param split_by: (list of (str,list)) Tags to split by.
    :param tool_class: (list) Tool instances.
    :param stage_name: (str) The name of the stage to add to.  Defaults to the name of the tool class.
    :return: (list) The tools added.

    >>> dag() |Split| ([('shape',['square','circle']),('color',['red','blue'])],Tool_Class)
    """
    parent_tools = dag.last_tools
    splits = [ list(it.product([split[0]],split[1])) for split in split_by ] #splits = [[(key1,val1),(key1,val2),(key1,val3)],[(key2,val1),(key2,val2),(key2,val3)],[...]]
    for parent_tool in parent_tools:
        for new_tags in it.product(*splits):
            tags = dict(parent_tool.tags).copy()
            tags.update(dict(new_tags))
            new_tool = tool_class(stage_name=stage_name,dag=dag,tags=tags) 
            dag.G.add_edge(parent_tool,new_tool)
            yield new_tool
Split = Infix(_split)


@flowfxn
def _reduce(dag,keywords,tool_class,stage_name=None):
    """
    Create new tools with a many2one to parent_tools.

    :param dag: (dag) The dag to add to.
    :param parent_tools: (list) A list of parent tools.
    :param keywords: (list of str) Tags to reduce to.  All keywords not listed will not be passed on to the tasks generated.
    :param tool_class: (list) Tool instances.
    :param stage_name: (str) The name of the stage to add to.  Defaults to the name of the tool class.
    :return: (list) The tools added.

    >>> dag() |Reduce| (['shape'],Tool_Class)
    """
    parent_tools = dag.last_tools
    if type(keywords) != list:
        raise dagError('Invalid Right Hand Side of reduce')
    for tags, parent_tool_group in groupby(parent_tools,lambda t: dict([(k,t.tags[k]) for k in keywords])):
        parent_tool_group = list(parent_tool_group)
        new_tool = tool_class(stage_name=stage_name,dag=dag,tags=tags)
        for parent_tool in parent_tool_group:
            dag.G.add_edge(parent_tool,new_tool)
        yield new_tool
Reduce = Infix(_reduce)

@flowfxn
def _reduce_and_split(dag,keywords,split_by,tool_class,stage_name=None):
    """
    Create new tools by first reducing then splitting.

    :param dag: (dag) The dag to add to.
    :param parent_tools: (list) A list of parent tools.
    :param keywords: (list of str) Tags to reduce to.  All keywords not listed will not be passed on to the tasks generated.
    :param split_by: (list of (str,list)) Tags to split by.  Creates every possible product of the tags.
    :param tool_class: (list) Tool instances.
    :param stage_name: (str) The name of the stage to add to.  Defaults to the name of the tool class.
    :return: (list) The tools added.

    >>> dag() |ReduceSplit| (['color','shape'],[(size,['small','large'])],Tool_Class)
    """
    parent_tools = dag.last_tools
    splits = [ list(it.product([split[0]],split[1])) for split in split_by ] #splits = [[(key1,val1),(key1,val2),(key1,val3)],[(key2,val1),(key2,val2),(key2,val3)],[...]]
    
    for group_tags,parent_tool_group in groupby(parent_tools,lambda t: dict([(k,t.tags[k]) for k in keywords])):
        parent_tool_group = list(parent_tool_group)
        for new_tags in it.product(*splits):
            tags = group_tags.copy()
            tags.update(dict(new_tags))
            new_tool = tool_class(stage_name=stage_name,dag=dag,tags=tags)
            for parent_tool in parent_tool_group:
                dag.G.add_edge(parent_tool,new_tool)
            yield new_tool
ReduceSplit = Infix(_reduce_and_split)

    