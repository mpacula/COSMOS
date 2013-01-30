"""
WGA Workflow

Input Dicts

[
    {
        'lane': 001,
        'chunk': 001,
        'library': 'LIB-1216301779A',
        'sample': '1216301779A',
        'platform': 'ILLUMINA',
        'flowcell': 'C0MR3ACXX'
        'pair': 0, #0 or 1
        'path': '/path/to/fastq'
    },
    {..}
]
"""

_author_ = 'Erik Gafni'

from cosmos import session
from cosmos.Workflow.models import Workflow
from cosmos.contrib.ezflow.dag import DAG, Apply, Reduce, Split, ReduceSplit, Add
from cosmos.contrib.ezflow.tool import INPUT
from tools import gatk,picard,bwa
from inputs.read import get_inputs
from cosmos.Workflow.models import TaskFile
from cosmos.Workflow.cli import CLI

import json
from settings import settings
import os

####################
# Input
####################

cli = CLI()
cli.parser.add_argument('-i','--inputs',type=str,help='Inputs, see script comments for format.',required=True)
cli.parser.add_argument('-p','--input_path',type=str,help='Prepends a directory to all input paths.')

WF = cli.parse_args()


#setup inputs
input_path = cli.parsed_kwargs['input_path']
with open(cli.parsed_kwargs['inputs'],'r') as input_dict:
    input_list = json.loads(input_dict.read())
    if input_path:
        for i in input_list:
            i['path'] = os.path.join(input_path,i['path'])

inputs = [ INPUT(taskfile=TaskFile(name='fastq.gz',path=i['path'],fmt='fastq.gz'),tags=i) for i in input_list ]
inputs = filter(lambda i: i.tags['chunk'] == '001',inputs)

####################
# Configuration
####################

#parameter keywords can be the name of the tool class, or its default __verbose__
parameters = {
  'ALN': { 'q': 5 },
}

# Tags
intervals = ('interval',range(1,23)+['X','Y'])
glm = ('glm',['SNP','INDEL'])
dbs = ('database',['1000G','PolyPhen2','COSMIC','ENCODE'])

####################
# Create DAG
####################

dag = (DAG(mem_req_factor=1)
    |Add| inputs
    |Apply| bwa.ALN
    |Reduce| (['sample','flowcell','lane','chunk'],bwa.SAMPE)
    |Apply| picard.CLEAN_SAM
    |Reduce| (['sample'],picard.MERGE_SAMS)
    |Apply| picard.INDEX_BAM
    |Split| ([intervals],gatk.RTC)
    |Apply| gatk.IR
    |Reduce| (['sample'],gatk.BQSR)
    |Apply| gatk.PR
    |ReduceSplit| ([],[glm,intervals], gatk.UG)
    |Reduce| (['glm'],gatk.CV)
    |Apply| gatk.VQSR
    |Apply| gatk.Apply_VQSR
    |Reduce| ([],gatk.CV,"CV 2")
#    |Split| ([dbs],annotate.ANNOVAR)
#    |Apply| annotate.PROCESS_ANNOVAR
#    |Reduce| ([],annotate.MERGE_ANNOTATIONS)
#    |Apply| annotate.SQL_DUMP
#    |Apply| annotate.ANALYSIS
)
dag.configure(settings,parameters)
dag.create_dag_img('/tmp/graph.svg')

#################
# Run Workflow
#################

dag.add_to_workflow(WF)
WF.run()