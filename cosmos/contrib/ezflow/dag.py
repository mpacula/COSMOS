from cosmos.Cosmos.helpers import groupby
import itertools as it
import networkx as nx
import pygraphviz as pgv
from cosmos.Workflow.models import Task,TaskError,TaskFile
from tool import INPUT
from picklefield.fields import dbsafe_decode

class DAGError(Exception): pass

class DAG(object):
    
    def __init__(self):
        self.G = nx.DiGraph()
        self.last_tools = []
        
    def create_dag_img(self,path):
        AG = pgv.AGraph(strict=False,directed=True,fontname="Courier",fontsize=11)
        AG.node_attr['fontname']="Courier-Bold"
        AG.node_attr['fontsize']=12
            
        for tool in self.G.nodes():
            AG.add_node(tool,label=tool.label)
        AG.add_edges_from(self.G.edges())
        AG.layout(prog="dot")
        AG.draw(path,format='svg')
        print 'wrote to {0}'.format(path)
    
    def describe(self,generator):
        return list(generator)
        
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
                
            tool.parameters = self.parameters[tool.stage_name]
            
    def add_to_workflow(self,WF):
        #this assumes WF has resently been reloaded, so all tasks in the DB are successful.
        OPnodes = filter(lambda x: not x.NOOP,self.G.nodes())
        OPedges = filter(lambda x: not x[0].NOOP and not x[1].NOOP,self.G.edges())
        
        #Add stages, and set the tool.stage reference for all tools
        stages = {}
        for tool in nx.topological_sort(self.G):
            tool.in_database = False
            if not tool.NOOP:
                stage_name = tool.stage_name
                if stage_name not in stages: #have not seen this stage yet
                    stages[stage_name] = WF.add_stage(stage_name)
                tool.stage = stages[stage_name]
        
        """
        Update with the data that is already in the database
        """
        all_tags = map(lambda n: n.tags, self.G.node)
        #validate all_tags has no duplicates
        if len(set(all_tags)) != len(all_tags): raise DAGError('Duplicate tags detected!')
        stasks = list(Task.objects.all().select_related('_output_files','stage'))
        for tpl, group in groupby(stasks + self.G.nodes(), lambda x: (x.tags,x.stage.name)):
            group = list(group)
            if len(group) >1:
                tags = tpl[0]
                stage_name = tpl[1]
                tool = group[0] if isinstance(group[0],Task) else group[1]
                task = group[0] if isinstance(group[0],Task) else group[1]
                tool._output_files = task.output_files
                tool.in_database = True
                tool._task_instance = task
        
        
        new_nodes = filter(lambda n: not n.in_database, self.G.nodes())
        
        #bulk save task_files.  All inputs have to at some point be an output, so just bulk save the outputs
        taskfiles = list(it.chain(*[ n.output_files for n in new_nodes ]))
        WF.bulk_save_task_files(taskfiles)
        
        #bulk save tasks
        for node in new_nodes:
                node._task_instance = self.__new_task(WF,node.stage,node)
        
        tasks = [ node._task_instance for node in OPnodes ]
        WF.bulk_save_tasks(tasks)
        
        ### Bulk add task->output_taskfile relationships
        ThroughModel = Task._output_files.through
        rels = [ ThroughModel(task_id=n._task_instance.id,taskfile_id=out.id) for n in OPnodes for out in n.output_files ]
        ThroughModel.objects.bulk_create(rels)
        
        #bulk save edges
        task_edges = [ (parent._task_instance,child._task_instance) for parent,child in OPedges ]
        WF.bulk_save_task_edges(task_edges)
    
    def __new_task(self,workflow,stage,task):
        """adds a task"""
        try:
            return stage.new_task(name = '',
                                  pcmd = task.pcmd,
                                  tags = task.tags,
                                  input_files = task.input_files,
                                  output_files = task.output_files,
                                  mem_req = task.mem_req,
                                  cpu_req = task.cpu_req)
        except TaskError as e:
            raise TaskError('{0}. Task is {1}.'.format(e,task))
            


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
    1) If the second argument is a tuple (ie multiple args submitted with infix notation), submit it as *args
    2) The decorated function should return a genorator, evaluate it
    3) Set the dag.last_tools to the decorated function's return value
    4) Return the dag
    """
    def wrapped(*args,**kwargs):
        #TODO confirm args[0] is a dag
        LHS = args[0]
        RHS = args[1] if type(args[1]) == tuple else (args[1],)
        try:
            LHS.last_tools = list(func(LHS,*RHS))
            return LHS
        except TypeError:
            raise
#            raise dagError('Func {0} called with arguments {1} and *{2}'.format(func,LHS,RHS))
    return wrapped

@infix
def _add(dag,tool_instance_list):
    for i in tool_instance_list:
        dag.G.add_node(i)
        yield i
Add = Infix(_add)

@infix
def _apply(dag,tool_class,stage_name=None):
    input_tools = dag.last_tools
    #TODO validate that tool_class.stage_name is unique
    for input_tool in input_tools:
        new_tool = tool_class(stage_name=stage_name,dag=dag,tags=input_tool.tags)
        dag.G.add_edge(input_tool,new_tool)
        yield new_tool
        
Apply = Infix(_apply) #map

@infix
def _reduce(dag,keywords,tool_class,stage_name=None):
    input_tools = dag.last_tools
    if type(keywords) != list:
        raise dagError('Invalid Right Hand Side of reduce')
    for tags, input_tool_group in groupby(input_tools,lambda t: dict([(k,t.tags[k]) for k in keywords])):
        input_tool_group = list(input_tool_group)
        new_tool = tool_class(stage_name=stage_name,dag=dag,tags=tags)
        for input_tool in input_tool_group:
            dag.G.add_edge(input_tool,new_tool)
        yield new_tool
Reduce = Infix(_reduce)

@infix
def _split(dag,split_by,tool_class,stage_name=None):
    input_tools = dag.last_tools
    splits = [ list(it.product([split[0]],split[1])) for split in split_by ] #splits = [[(key1,val1),(key1,val2),(key1,val3)],[(key2,val1),(key2,val2),(key2,val3)],[...]]
    for input_tool in input_tools:
        for new_tags in it.product(*splits):
            tags = tags=merge_dicts(dict(input_tool.tags),dict(new_tags))
            new_tool = tool_class(stage_name=stage_name,dag=dag,tags=tags) 
            dag.G.add_edge(input_tool,new_tool)
            yield new_tool
Split = Infix(_split)

@infix
def _reduce_and_split(dag,keywords,split_by,tool_class,stage_name=None):
    input_tools = dag.last_tools
    splits = [ list(it.product([split[0]],split[1])) for split in split_by ] #splits = [[(key1,val1),(key1,val2),(key1,val3)],[(key2,val1),(key2,val2),(key2,val3)],[...]]
    
    for group_tags,input_tool_group in groupby(input_tools,lambda t: dict([(k,t.tags[k]) for k in keywords])):
        input_tool_group = list(input_tool_group)
        for new_tags in it.product(*splits):
            new_tool = tool_class(stage_name=stage_name,dag=dag,tags=merge_dicts(group_tags,dict(new_tags)))
            for input_tool in input_tool_group:
                dag.G.add_edge(input_tool,new_tool)
            yield new_tool
ReduceSplit = Infix(_reduce_and_split)


    