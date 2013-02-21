from cosmos.utils.helpers import groupby
import itertools as it
import networkx as nx
import pygraphviz as pgv
from cosmos.Workflow.models import Task,TaskError
from picklefield.fields import dbsafe_decode

class DAGError(Exception): pass

class DAG(object):
    
    def __init__(self,cpu_req_override=False,mem_req_factor=1):
        """
        :param: set to an integer to override all task cpu_requirements.  Useful when a :term:`DRM` does not support requesting multiple cpus
        :param mem_req_factor: multiply all task mem_reqs by this number.
        """
        self.G = nx.DiGraph()
        self.last_tools = []
        self.cpu_req_override = cpu_req_override
        self.mem_req_factor = mem_req_factor
        self.stage_names_used = []

    def use(self,stage_name):
        """
        Updates last_tools to be the tools in the stage with name stage_name.  This way the infix operations
        can be applied to multiple stages if the workflow isn't "linear".

        :param stage_name: (str) the name of the stage
        :return:the updated dag.
        """
        if stage_name not in self.stage_names_used:
            print self.stage_names_used
            raise KeyError, 'Stage name "{0}" does not exist.'.format(stage_name)
        self.last_tools = filter(lambda n: n.stage_name == stage_name, self.G.nodes())
        return self
        
    def create_dag_img(self,path):
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
        Sets the parameters of every tool in the dag
        
        :param params: (dict) {'stage_name': { params_dict }, {'stage_name2': { param_dict2 } }
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
            
    def add_to_workflow(self,WF):
        #add new tasks and related objects to WF
        WF.log.info('Adding tasks to workflow.')
        
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
                stages[stage_name] = WF.add_stage(stage_name)
            tool.stage = stages[stage_name]
        
        #update tool._task_instance and tool._output_files with existing data
        stasks = list(WF.tasks.select_related('_output_files','stage'))
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
        WF.log.info('Total tasks: {0}, New tasks being added: {1}'.format(len(self.G.nodes()),len(new_nodes)))
        
        #bulk save task_files.  All inputs have to at some point be an output, so just bulk save the outputs.
        #Must come before adding tasks, since taskfile.ids must be populated to compute the proper pcmd.
        taskfiles = list(it.chain(*[ n.output_files for n in new_nodes ]))

        WF.bulk_save_task_files(taskfiles)
        
        #bulk save tasks
        for node in new_nodes:
                node._task_instance = self.__new_task(WF,node.stage,node)
        tasks = [ node._task_instance for node in new_nodes ]
        WF.bulk_save_tasks(tasks)
        
        ### Bulk add task->output_taskfile relationships
        ThroughModel = Task._output_files.through
        rels = [ ThroughModel(task_id=n._task_instance.id,taskfile_id=out.id) for n in new_nodes for out in n.output_files ]
        ThroughModel.objects.bulk_create(rels)
        
        #bulk save edges
        new_edges = filter(lambda e: e[0] in new_nodes or e[1] in new_nodes,self.G.edges())
        task_edges = [ (parent._task_instance,child._task_instance) for parent,child in new_edges ]
        WF.bulk_save_task_edges(task_edges)
    
    def __new_task(self,workflow,stage,tool):
        """adds a task"""
        try:
            return stage.new_task(name = '',
                                  pcmd = tool.pcmd,
                                  tags = tool.tags,
                                  input_files = tool.input_files,
                                  output_files = tool.output_files,
                                  mem_req = tool.mem_req * self.mem_req_factor,
                                  cpu_req = tool.cpu_req if not self.cpu_req_override else self.cpu_req_override,
                                  time_req = tool.time_req,
                                  NOOP = tool.NOOP,
                                  dont_delete_output_files = tool.dont_delete_output_files,
                                  succeed_on_failure = tool.succeed_on_failure)
        except TaskError as e:
            raise TaskError('{0}. Task is {1}.'.format(e,tool))
            


class dagError(Exception):pass

def merge_dicts(*args):
    """
    Merges dictionaries in *args.  On duplicate keys, right most dict take precedence
    """
    def md(x,y):
        x = x.copy()
        for k,v in y.items(): x[k]=v
        return x
    return reduce(md,args)

class Infix:
    def __init__(self, function):
        self.function = function
    def __ror__(self, other):
        return Infix(lambda x, self=self, other=other: self.function(other, x))
    def __or__(self, other):
        return self.function(other)
    def __rlshift__(self, other):
        return Infix(lambda x, self=self, other=other: self.function(other, x))
    def __rshift__(self, other):
        return self.function(other)
    def __call__(self, value1, value2):
        return self.function(value1, value2)
    
WF = None

def infix(func,*args,**kwargs):
    """
    1) Set RHS_item to be tuples if they're not already
    2) The decorated function should return a generator, so evaluate it
    3) Set the dag.last_tools to the decorated function's return value
    4) Return the dag
    """
    def wrapped(*args,**kwargs):
        #TODO confirm args[0] is a dag
        dag = args[0]
        RHS = args[1] if type(args[1]) == tuple else (args[1],) #make sure RHS_item is a tuple
        try:
            dag.last_tools = list(func(dag,dag.last_tools,*RHS))
        except TypeError:
            raise

        stage_name = dag.last_tools[0].stage_name
        assert stage_name not in dag.stage_names_used, 'Duplicate stage_names detected {0}.'.format(stage_name)
        dag.stage_names_used.append(stage_name)

        return dag
    return wrapped

@infix
def _add(dag,parent_tools,tools,stage_name=None):
    """
    Add a list of tool instances with no dependencies

    :param dag: The dag to add to.
    :param parent_tools: Not used.
    :param tools: (list) tool instansces.
    :return: (list) the tools added.
    """
    for i in tools:
        dag.G.add_node(i)
        yield i
Add = Infix(_add)

@infix
def _apply(dag,parent_tools,tool_class,stage_name=None):
    """
    Create one2one relationships for all parent_tools using tool_class
    """
    for parent_tool in parent_tools:
        new_tool = tool_class(stage_name=stage_name,dag=dag,tags=parent_tool.tags)
        dag.G.add_edge(parent_tool,new_tool)
        yield new_tool
        
Apply = Infix(_apply) #map

@infix
def _reduce(dag,parent_tools,keywords,tool_class,stage_name=None):
    """
    Create a many2one relationship.s
    :param keywords: Tags to reduce to.  All keywords not listed will not be passed on to the tasks generated.x
    """
    if type(keywords) != list:
        raise dagError('Invalid Right Hand Side of reduce')
    for tags, parent_tool_group in groupby(parent_tools,lambda t: dict([(k,t.tags[k]) for k in keywords])):
        parent_tool_group = list(parent_tool_group)
        new_tool = tool_class(stage_name=stage_name,dag=dag,tags=tags)
        for parent_tool in parent_tool_group:
            dag.G.add_edge(parent_tool,new_tool)
        yield new_tool
Reduce = Infix(_reduce)

#TODO raise exceptions if user submits bad kwargs for any infix commands
@infix
def _split(dag,parent_tools,split_by,tool_class,stage_name=None):
    """
    one2manys
    """
    splits = [ list(it.product([split[0]],split[1])) for split in split_by ] #splits = [[(key1,val1),(key1,val2),(key1,val3)],[(key2,val1),(key2,val2),(key2,val3)],[...]]
    for parent_tool in parent_tools:
        for new_tags in it.product(*splits):
            tags = tags=merge_dicts(dict(parent_tool.tags),dict(new_tags))
            new_tool = tool_class(stage_name=stage_name,dag=dag,tags=tags) 
            dag.G.add_edge(parent_tool,new_tool)
            yield new_tool
Split = Infix(_split)

@infix
def _reduce_and_split(dag,parent_tools,keywords,split_by,tool_class,stage_name=None):
    """
    many2many
    """
    splits = [ list(it.product([split[0]],split[1])) for split in split_by ] #splits = [[(key1,val1),(key1,val2),(key1,val3)],[(key2,val1),(key2,val2),(key2,val3)],[...]]
    
    for group_tags,parent_tool_group in groupby(parent_tools,lambda t: dict([(k,t.tags[k]) for k in keywords])):
        parent_tool_group = list(parent_tool_group)
        for new_tags in it.product(*splits):
            new_tool = tool_class(stage_name=stage_name,dag=dag,tags=merge_dicts(group_tags,dict(new_tags)))
            for parent_tool in parent_tool_group:
                dag.G.add_edge(parent_tool,new_tool)
            yield new_tool
ReduceSplit = Infix(_reduce_and_split)

# class args(object):
#     "argument object"
#     def __init__(self,*args,**kwargs):
#         self.args = args
#         self.kwargs = kwargs
#

    