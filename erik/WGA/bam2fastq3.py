__author__ = 'erik'

"""
Convert a Bam to Fastqs
"""

from cosmos import session
from cosmos.contrib.ezflow.dag import DAG, Apply, Reduce, Split, ReduceSplit, Add
from cosmos.contrib.ezflow.tool import INPUT,Tool
from cosmos.Workflow.models import TaskFile
from cosmos.Workflow.cli import CLI
from tools import picard
import os
from scripts import rg_helpers
from settings import settings


class SplitFastq(Tool):
    inputs = ['_1.fastq','_2.fastq']
    outputs = ['dir']
    time_req = 0
    mem_req = 1000

    def cmd(self,i,s,p):
        input = i['_1.fastq'][0] if p['pair'] == 1 else i['_2.fastq'][0]
        return "python scripts/splitfastq.py {input} $OUT.dir", { 'input': input}

class FilterBamByRG(Tool):
    inputs = ['bam']
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
print 'RGIDS:'
print rgids
####################
# Create DAG
####################

dag = (DAG()
        |Add| [ INPUT(output_path=input_file) ]
        |Split| ([('rgid',rgids)],FilterBamByRG)
        |Apply| picard.REVERTSAM
        |Apply| picard.SAM2FASTQ
        |Split| ([('pair',[1,2])],SplitFastq)
    ).configure(settings=settings)

#################
# Run Workflow
#################

dag.add_to_workflow(WF)
WF.run()