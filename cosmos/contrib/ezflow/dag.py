import networkx as nx
import pygraphviz as pgv
from cosmos.Cosmos.helpers import groupby
from cosmos.util.typecheck import accepts, returns
import itertools as it
import sys

class WorkflowDAG(object):
    
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
            task.parameters = params.get(task.stage_name,{'stage_name':task.stage_name})
            
    def add_to_workflow(self,WF):
        for stage_name, nodes in groupby(self.G.node,lambda x: x.stage_name):
            print stage_name
            print list(nodes)
        #print self.G.in_degree().items()
        #degree_0_tasks = map(lambda x:x[0],filter(lambda x: x[1] == 0,self.G.in_degree().items()))
        #print degree_0_tasks


class FlowException(Exception):pass


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

def _apply(input_tasks,Task_CLS):
    #TODO validate that Task_CLS.stage_name is unique in all infix operations
    for input_task in input_tasks:
        DAG = input_task.DAG
        new_task = Task_CLS(DAG=DAG,tags=input_task.tags)
        DAG.G.add_edge(input_task,new_task)
        yield new_task
Apply = Infix(_apply) #map

def _reduce(input_tasks,RHS):
    try:
        if len(RHS) != 2 or type(RHS[0]) != list:
            raise FlowException('Invalid Right Hand Side of reduce')
    except Exception:
        raise FlowException('Invalid Right Hand Side of reduce')
    keywords = RHS[0]
    Task_CLS = RHS[1]
    for tags, input_task_group in groupby(input_tasks,lambda t: dict([(k,t.tags[k]) for k in keywords])):
        input_task_group = list(input_task_group)
        DAG = input_task_group[0].DAG
        new_task = Task_CLS(DAG=DAG,tags=tags)
        for input_task in input_task_group:
            DAG.G.add_edge(input_task,new_task)
        yield new_task
Reduce = Infix(_reduce)

def _split(input_tasks,RHS):
    splits = [ list(it.product([split[0]],split[1])) for split in RHS[0] ] #splits = [[(key1,val1),(key1,val2),(key1,val3)],[(key2,val1),(key2,val2),(key2,val3)],[...]]
    Task_CLS = RHS[1]
    for input_task in input_tasks:
        DAG = input_task.DAG
        for new_tags in it.product(*splits):
            tags = tags=merge_dicts(dict(input_task.tags),dict(new_tags))
            new_task = Task_CLS(DAG=DAG,tags=tags) 
            DAG.G.add_edge(input_task,new_task)
            yield new_task
Split = Infix(_split)

def _reduce_and_split(input_tasks,RHS):
    keywords = RHS[0]
    splits = [ list(it.product([split[0]],split[1])) for split in RHS[1] ] #splits = [[(key1,val1),(key1,val2),(key1,val3)],[(key2,val1),(key2,val2),(key2,val3)],[...]]
    Task_CLS = RHS[2]
    for group_tags,input_task_group in groupby(input_tasks,lambda t: dict([(k,t.tags[k]) for k in keywords])):
        input_task_group = list(input_task_group)
        DAG = input_task_group[0].DAG
        for new_tags in it.product(*splits):
            new_task = Task_CLS(DAG=DAG,tags=merge_dicts(group_tags,dict(new_tags)))
            for input_task in input_task_group:
                DAG.G.add_edge(input_task,new_task)
            yield new_task
ReduceSplit = Infix(_reduce_and_split)


    