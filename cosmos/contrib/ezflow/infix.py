import networkx as nx
from cosmos.Cosmos.helpers import groupby
from task import *
import itertools as it

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
    

G = nx.DiGraph()

def _apply(input_nodes,task):
    for in_node in input_nodes:
        new_node = task(in_node.tags)
        G.add_edge(in_node,new_node)
        yield new_node
Apply = Infix(_apply) #map
        
def _reduce(input_nodes,RHS):
    keywords = RHS[0]
    task = RHS[1]
    for tags, input_node_group in groupby(input_nodes,lambda t: dict([(k,t.tags[k]) for k in keywords])):
        new_node = task(tags=tags)
        for input_node in input_node_group:
            G.add_edge(input_node,new_node)
        yield new_node
Reduce = Infix(_reduce)

def _split(input_nodes,RHS):
    splits = RHS[0]
    splits = [ (key,val) for key,vals in splits for val in vals ]
    task = RHS[1]
    for input_node in input_nodes:
        for new_tags in it.product(splits):
            new_node = task(tags=merge_dicts(dict(input_node.tags),dict(new_tags)))
            G.add_edge(input_node,new_node)
            yield new_node
Split = Infix(_split)
        
def _reduce_and_split(input_nodes,RHS):
    keywords = RHS[0]
    splits = RHS[1]
    splits = [ (key,val) for key,vals in splits for val in vals ]
    task = RHS[2]
    for group_tags,input_node_group in groupby(input_nodes,lambda t: dict([(k,t.tags[k]) for k in keywords])):
        input_node_group = list(input_node_group)
        for new_tags in it.product(splits):
            new_node = task(merge_dicts(group_tags,dict(new_tags)))
            for input_node in input_node_group:
                G.add_edge(input_node,new_node)
            yield new_node
        
ReduceSplit = Infix(_reduce_and_split)


INPUT = [
    Task(tags={'sample':'A',
    'fq_chunk':1,
    'fq_pair':1}),
    Task(tags={'sample':'A',
    'fq_chunk':1,
    'fq_pair':2}),
    Task(tags={'sample':'A',
    'fq_chunk':2,
    'fq_pair':1}),
    Task(tags={'sample':'A',
    'fq_chunk':2,
    'fq_pair':2}),
    Task(tags={'sample':'B',
    'fq_chunk':1,
    'fq_pair':1}),
    Task(tags={'sample':'B',
    'fq_chunk':1,
    'fq_pair':2}),
]
intervals = ('interval',[1,2,3,4])
glm = ('glm',['SNP','INDEL'])

run(
    INPUT
    |Apply| ALN |Reduce| (['sample','fq_chunk'],SAMPE) |Split| ([intervals],IRTC) |Apply| IR |ReduceSplit| ([],[intervals], UG)
) 
AG= nx.to_agraph(G)
AG.layout(prog="dot")
AG.draw('/tmp/graph.svg',format='svg')
print 'wrote to /tmp/graph.svg'
    