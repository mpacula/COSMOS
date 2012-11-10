import networkx as nx
import pygraphviz as pgv
from cosmos.Cosmos.helpers import groupby
from cosmos.util.typecheck import accepts, returns
import itertools as it

DAG = nx.DiGraph()

class FlowException(Exception):
    pass



def run(g):
    return list(g)

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
    

def _apply(input_nodes,task):
    for in_node in input_nodes:
        new_node = task(in_node.tags)
        DAG.add_edge(in_node,new_node)
        yield new_node
Apply = Infix(_apply) #map
        

def _reduce(input_nodes,RHS):
    try:
        if len(RHS) != 2 or type(RHS[0]) != list:
            raise FlowException('Invalid Right Hand Side of reduce')
    except Exception:
        raise FlowException('Invalid Right Hand Side of reduce')
    keywords = RHS[0]
    task = RHS[1]
    for tags, input_node_group in groupby(input_nodes,lambda t: dict([(k,t.tags[k]) for k in keywords])):
        new_node = task(tags=tags)
        for input_node in input_node_group:
            DAG.add_edge(input_node,new_node)
        yield new_node
Reduce = Infix(_reduce)


def _split(input_nodes,RHS):
    splits = [ list(it.product([split[0]],split[1])) for split in RHS[0] ] #splits = [[(key1,val1),(key1,val2),(key1,val3)],[(key2,val1),(key2,val2),(key2,val3)],[...]]
    task = RHS[1]
    for input_node in input_nodes:
        for new_tags in it.product(*splits):
            tags = tags=merge_dicts(dict(input_node.tags),dict(new_tags))
            new_node = task(tags=tags)
            DAG.add_edge(input_node,new_node)
            yield new_node
Split = Infix(_split)



def _reduce_and_split(input_nodes,RHS):
    keywords = RHS[0]
    splits = [ list(it.product([split[0]],split[1])) for split in RHS[1] ] #splits = [[(key1,val1),(key1,val2),(key1,val3)],[(key2,val1),(key2,val2),(key2,val3)],[...]]
    task = RHS[2]
    for group_tags,input_node_group in groupby(input_nodes,lambda t: dict([(k,t.tags[k]) for k in keywords])):
        input_node_group = list(input_node_group)
        for new_tags in it.product(*splits):
            new_node = task(merge_dicts(group_tags,dict(new_tags)))
            for input_node in input_node_group:
                DAG.add_edge(input_node,new_node)
            yield new_node
ReduceSplit = Infix(_reduce_and_split)

def create_dag_img():
    global DAG
    AG = pgv.AGraph(strict=False,directed=True,fontname="Courier",fontsize=11)
    AG.node_attr['fontname']="Courier-Bold"
    AG.node_attr['fontsize']=12
        
    for node,data in DAG.nodes(data=True):
        AG.add_node(node,label=node.label)
    AG.add_edges_from(DAG.edges())
    AG.layout(prog="dot")
    AG.draw('/tmp/graph.svg',format='svg')
    print 'wrote to /tmp/graph.svg'

    