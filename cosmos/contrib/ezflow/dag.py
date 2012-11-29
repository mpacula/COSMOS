from cosmos.Cosmos.helpers import groupby
import itertools as it
import networkx as nx
import pygraphviz as pgv
from cosmos.Workflow.models import Task,TaskError,TaskFile
from tool import INPUT

class DAGError(Exception): pass

class DAG(object):
    
    def __init__(self):
        self.G = nx.DiGraph()
        self.last_tasks = []
        
    def add_input(self,tags,filepaths):
        #validate
        for t in self.last_tasks:
            if t.__class__.__name__ != 'INPUT':
                raise dagError('You must add all inputs during dag initialization')
            
        tfs = [ TaskFile(path=f) for f in filepaths ]
        i = INPUT(dag=self,tags=tags,stage_name='INPUT')
        
        #set i.output_files and i.outputs
        for tf in tfs:
            print tf
            i.output_files[tf.name] = tf
        i.outputs = [ tf.fmt for tf in tfs ]
        
        self.G.add_node(i)
        self.last_tasks.append(i)
        
    def create_dag_img(self):
        AG = pgv.AGraph(strict=False,directed=True,fontname="Courier",fontsize=11)
        AG.node_attr['fontname']="Courier-Bold"
        AG.node_attr['fontsize']=12
            
        for task,data in self.G.nodes(data=True):
            AG.add_node(task,label=task.label)
        AG.add_edges_from(self.G.edges())
        AG.layout(prog="dot")
        AG.draw('/tmp/graph.svg',format='svg')
        print 'wrote to ~/graph.svg'
    
    def describe(self,generator):
        return list(generator)
        
    def set_parameters(self,params):
        """
        Sets the parameters of every task in the dag
        
        :param params: (dict) {'stage_name': { params_dict }, {'stage_name2': { param_dict2 } }
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
    2) The decorated function should return a genorator, so evaluate it
    3) Set the dag.last_tasks to the decorated function's return value
    4) Return the dag
    """
    def wrapped(*args,**kwargs):
        #TODO confirm args[0] is a dag
        LHS = args[0]
        RHS = args[1] if type(args[1]) == tuple else (args[1],)
        try:
            LHS.last_tasks = list(func(LHS,*RHS))
            return LHS
        except TypeError:
            raise
#            raise dagError('Func {0} called with arguments {1} and *{2}'.format(func,LHS,RHS))
    return wrapped

@infix
def _add(dag,tags_list,tool_class,stage_name=None):
    if not stage_name: stage_name = tool_class.__name__
    for tags in tags_list:
        new_task = tool_class(stage_name=stage_name,dag=dag,tags=tags)
        dag.G.add_node(new_task)
        yield new_task
Add = Infix(_add)

@infix
def _apply(dag,tool_class,stage_name=None):
    input_tasks = dag.last_tasks
    if not stage_name: stage_name = tool_class.__name__
    #TODO validate that tool_class.stage_name is unique
    for input_task in input_tasks:
        new_task = tool_class(stage_name=stage_name,dag=dag,tags=input_task.tags)
        dag.G.add_edge(input_task,new_task)
        yield new_task
        
Apply = Infix(_apply) #map

@infix
def _reduce(dag,keywords,tool_class,stage_name=None):
    input_tasks = dag.last_tasks
    if not stage_name: stage_name = tool_class.__name__
    try:
        if type(keywords) != list:
            raise dagError('Invalid Right Hand Side of reduce')
    except Exception:
        raise dagError('Invalid Right Hand Side of reduce')
    for tags, input_task_group in groupby(input_tasks,lambda t: dict([(k,t.tags[k]) for k in keywords])):
        input_task_group = list(input_task_group)
        new_task = tool_class(stage_name=stage_name,dag=dag,tags=tags)
        for input_task in input_task_group:
            dag.G.add_edge(input_task,new_task)
        yield new_task
Reduce = Infix(_reduce)

@infix
def _split(dag,split_by,tool_class,stage_name=None):
    input_tasks = dag.last_tasks
    if not stage_name: stage_name = tool_class.__name__
    splits = [ list(it.product([split[0]],split[1])) for split in split_by ] #splits = [[(key1,val1),(key1,val2),(key1,val3)],[(key2,val1),(key2,val2),(key2,val3)],[...]]
    for input_task in input_tasks:
        for new_tags in it.product(*splits):
            tags = tags=merge_dicts(dict(input_task.tags),dict(new_tags))
            new_task = tool_class(stage_name=stage_name,dag=dag,tags=tags) 
            dag.G.add_edge(input_task,new_task)
            yield new_task
Split = Infix(_split)

@infix
def _reduce_and_split(dag,keywords,split_by,tool_class,stage_name=None):
    input_tasks = dag.last_tasks
    if not stage_name: stage_name = tool_class.__name__
    splits = [ list(it.product([split[0]],split[1])) for split in split_by ] #splits = [[(key1,val1),(key1,val2),(key1,val3)],[(key2,val1),(key2,val2),(key2,val3)],[...]]
    
    for group_tags,input_task_group in groupby(input_tasks,lambda t: dict([(k,t.tags[k]) for k in keywords])):
        input_task_group = list(input_task_group)
        for new_tags in it.product(*splits):
            new_task = tool_class(stage_name=stage_name,dag=dag,tags=merge_dicts(group_tags,dict(new_tags)))
            for input_task in input_task_group:
                dag.G.add_edge(input_task,new_task)
            yield new_task
ReduceSplit = Infix(_reduce_and_split)


    