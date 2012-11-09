from flow import G, Apply, Reduce, Split, ReduceSplit, run
from task import *
import networkx as nx
print 'test'
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