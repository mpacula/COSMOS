__author__ = 'erik'

"""
Convert a Bam to Fastqs
"""

from cosmos.contrib.ezflow.dag import DAG, Map, Reduce, Split, ReduceSplit, Add
from cosmos.contrib.ezflow.tool import INPUT,Tool
from wga.tools import picard
import os
import re

####################
# Tools
####################

class SplitFastq(Tool):
    inputs = ['1.fastq','2.fastq']
    outputs = ['dir']
    time_req = 120
    mem_req = 1000

    def cmd(self,i,s,p):
        input = i['1.fastq'][0] if p['pair'] == 1 else i['2.fastq'][0]
        return "python scripts/splitfastq.py {input} $OUT.dir", { 'input': input}

class FilterBamByRG(Tool):
    inputs = ['bam']
    outputs = ['bam']
    time_req = 120
    mem_req = 3000

    def cmd(self,i,s,p):
        return "{s[samtools_path]} view -h -b -r {p[rgid]} {i[bam][0]} -o $OUT.bam"


def Bam2Fastq(workflow,dag,settings,rgids):

    dag = (dag
            |Split| ([('rgid',rgids)],FilterBamByRG)
            |Map| picard.REVERTSAM
            |Map| picard.SAM2FASTQ
            |Split| ([('pair',[1,2])],SplitFastq)
        ).configure(settings=settings)
    dag.add_to_workflow(workflow)
    workflow.run(finish=False)

    #Load Fastq Chunks for processing
    input_chunks = []
    for input_tool in dag.last_tools:
        d = input_tool.tags.copy()
        #TODO tags should be set and inherited by the original bam
        d['sample'] = 'NA12878'
        d['library'] = 'LIB-NA12878'
        d['platform'] = 'ILLUMINA'

        d['flowcell'] = d['rgid'][:5]
        d['lane'] = d['rgid'][6:]
        for f in os.listdir(input_tool.output_files[0].path):
            path = os.path.join(input_tool.output_files[0].path,f)
            d2 = d.copy()
            d2['chunk'] = re.search("(\d+)\.fastq",f).group(1)
            new_tool = INPUT(path,tags=d2,stage_name='Load FASTQ Chunks')
            dag.G.add_edge(input_tool,new_tool)
            input_chunks.append(new_tool)
    dag.last_tools = input_chunks
    return dag