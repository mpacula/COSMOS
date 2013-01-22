__author__ = 'erik'

"""
Convert a Bam to Fastqs
"""

from cosmos import session
from cosmos.contrib.ezflow.dag import DAG, Apply, Reduce, Split, ReduceSplit, Add
from cosmos.contrib.ezflow.tool import INPUT,Tool
from cosmos.Workflow.cli import CLI
from tools import picard
import os
from scripts import rg_helpers
from settings import settings


class SplitFastq(Tool):
    outputs = ['dir']
    time_req = 0
    mem_req = 1000
    one_parent=True

    def cmd(self,i,s,p):
        return "python scripts/splitfastq.py {t[input]} $OUT.dir"

class FilterBamByRG(Tool):
    inputs=['bam']
    outputs = ['bam']
    time_req = 0
    mem_req = 5000
    one_parent=True

    def cmd(self,i,s,p):
        return "{s[samtools_path]} view -h -b -r {p[rgid]} {i[bam]} -o $OUT.bam"


cli = CLI()
cli.parser.add_argument('-i','--input_file',required=True)
WF = cli.parse_args()

input_file = cli.parsed_kwargs['input_file']
rgids = list(rg_helpers.list_rgids(input_file,settings['samtools_path']))
print rgids
####################
# Create DAG
####################

dag = (DAG()
        |Add| [ INPUT(output_path=input_file) ]
        |Split| ([('rgid',rgids)],FilterBamByRG)
        |Apply| picard.REVERTSAM
        |Apply| picard.SAM2FASTQ
    ).configure(settings=settings)

#################
# Run Workflow
#################

dag.add_to_workflow(WF)
WF.run(finish=False)

# Workflow part2: Split Fastqs
for input_tool in filter(lambda tool: tool.__class__.__name__ == 'SAM2FASTQ',dag.G.nodes()):
    input_dir = WF.stages.get(name='SAM2FASTQ').tasks.get(tags=input_tool.tags).output_files[0].path
    for fq in os.listdir(input_dir):
        dag.G.add_edge(input_tool,SplitFastq(dag=dag,tags={'input':os.path.join(input_dir,fq)}))

dag.add_to_workflow(WF)
WF.run()