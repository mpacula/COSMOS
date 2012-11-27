from cosmos.Cosmos.helpers import groupby
import itertools as it
import networkx as nx
import pygraphviz as pgv
from cosmos.Workflow.models import Task,TaskError

class TaskDAG(object):
    
    def __init__(self):
        self.G = nx.DiGraph()
        
    def create_dag_img(self):
        AG = pgv.AGraph(strict=False,directed=True,fontname="Courier",fontsize=11)
        AG.node_attr['fontname']="Courier-Bold"
        AG.node_attr['fontsize']=12
            
        for task,data in self.G.nodes(data=True):
            AG.add_node(task,label=task.label)
        AG.add_edges_from(self.G.edges())
        AG.layout(prog="dot")
        AG.draw('/tmp/graph.svg',format='svg')
        print 'wrote to /tmp/graph.svg'
        
    def describe(self,generator):
        return list(generator)
        
    def set_parameters(self,params):
        """
        Sets the parameters of every task in the dag
        """
        for task in self.G.node:
            task.parameters = params.get(task.stage_name,{})
            
    def add_to_workflow(self,WF):
        OPnodes = filter(lambda x: not x.NOOP,self.G.node.keys())
        OPedges = filter(lambda x: not x[0].NOOP and not x[1].NOOP,self.G.edges())
        
        for stage_name, nodes in groupby(self.G.node.items(),lambda t: t[0].stage_name):
            stage = WF.add_stage(stage_name)
            for n in nodes:
                n[1]['stage'] = stage
        
        #bulk save task_files.  All inputs have to at some point be an output, so just bulk save the outputs
        taskfiles = list(it.chain(*[ n.output_files.values() for n in self.G.node ]))
        WF.bulk_save_task_files(taskfiles)
        
        #bulk save tasks
        for node,attrs in self.G.node.items():
            if not node.NOOP:
                node._task_instance = self.__add_task_to_stage(WF,attrs['stage'],node)
        
        tasks = [ node._task_instance for node in OPnodes ]
        WF.bulk_save_tasks(tasks)
        
        ### Bulk add task->output_taskfile relationships
        ThroughModel = Task._output_files.through
        rels = [ ThroughModel(task_id=n._task_instance.id,taskfile_id=out.id) for n in OPnodes for out in n.output_files.values() ]
        ThroughModel.objects.bulk_create(rels)
        
        #bulk save edges
        task_edges = [ (parent._task_instance,child._task_instance) for parent,child in OPedges ]
        WF.bulk_save_task_edges(task_edges)
    
    def __add_task_to_stage(self,workflow,stage,task):
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
            


class DagError(Exception):pass

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
    If the second argument is a tuple, submit it as *args
    """
    def wrapped(*args,**kwargs):
        LHS = args[0]
        RHS = args[1] if type(args[1]) == tuple else (args[1],)
        try:
            return func(LHS,*RHS)
        except TypeError:
            raise DagError('Func {0} called with arguments {1} and *{2}'.format(func,LHS,RHS))
    return wrapped

@infix
def _apply(input_tasks,tool_class,stage_name=None):
    if not stage_name: stage_name = tool_class.__name__
    #TODO validate that tool_class.stage_name is unique
    for input_task in input_tasks:
        DAG = input_task.DAG
        new_task = tool_class(stage_name=stage_name,DAG=DAG,tags=input_task.tags)
        DAG.G.add_edge(input_task,new_task)
        yield new_task
        
Apply = Infix(_apply) #map

@infix
def _reduce(input_tasks,keywords,tool_class,stage_name=None):
    if not stage_name: stage_name = tool_class.__name__
    try:
        if type(keywords) != list:
            raise DagError('Invalid Right Hand Side of reduce')
    except Exception:
        raise DagError('Invalid Right Hand Side of reduce')
    for tags, input_task_group in groupby(input_tasks,lambda t: dict([(k,t.tags[k]) for k in keywords])):
        input_task_group = list(input_task_group)
        DAG = input_task_group[0].DAG
        new_task = tool_class(stage_name=stage_name,DAG=DAG,tags=tags)
        for input_task in input_task_group:
            DAG.G.add_edge(input_task,new_task)
        yield new_task
Reduce = Infix(_reduce)

@infix
def _split(input_tasks,split_by,tool_class,stage_name=None):
    if not stage_name: stage_name = tool_class.__name__
    splits = [ list(it.product([split[0]],split[1])) for split in split_by ] #splits = [[(key1,val1),(key1,val2),(key1,val3)],[(key2,val1),(key2,val2),(key2,val3)],[...]]
    for input_task in input_tasks:
        DAG = input_task.DAG
        for new_tags in it.product(*splits):
            tags = tags=merge_dicts(dict(input_task.tags),dict(new_tags))
            new_task = tool_class(stage_name=stage_name,DAG=DAG,tags=tags) 
            DAG.G.add_edge(input_task,new_task)
            yield new_task
Split = Infix(_split)

@infix
def _reduce_and_split(input_tasks,keywords,split_by,tool_class,stage_name=None):
    if not stage_name: stage_name = tool_class.__name__
    splits = [ list(it.product([split[0]],split[1])) for split in split_by ] #splits = [[(key1,val1),(key1,val2),(key1,val3)],[(key2,val1),(key2,val2),(key2,val3)],[...]]
    
    for group_tags,input_task_group in groupby(input_tasks,lambda t: dict([(k,t.tags[k]) for k in keywords])):
        input_task_group = list(input_task_group)
        DAG = input_task_group[0].DAG
        for new_tags in it.product(*splits):
            new_task = tool_class(stage_name=stage_name,DAG=DAG,tags=merge_dicts(group_tags,dict(new_tags)))
            for input_task in input_task_group:
                DAG.G.add_edge(input_task,new_task)
            yield new_task
ReduceSplit = Infix(_reduce_and_split)


    