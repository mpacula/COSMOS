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
from settings import settings
import re
import json


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


def Bam2Fastq(dag,settings,rgids):

    dag = (dag
            |Split| ([('rgid',rgids)],FilterBamByRG)
            |Apply| picard.REVERTSAM
            |Apply| picard.SAM2FASTQ
            |Split| ([('pair',[1,2])],SplitFastq)
        ).configure(settings=settings)
