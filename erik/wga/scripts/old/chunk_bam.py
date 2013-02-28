__author__ = 'erik'

"""
Chunks a Bam
"""
from cosmos import session
from cosmos.Workflow.models import Workflow
from cosmos.contrib.ezflow.dag import DAG, Map, Reduce, Split, ReduceSplit, Add
from cosmos.contrib.ezflow.tool import INPUT,Tool
from tools import picard
import os
from settings import settings
from cosmos.Workflow.cli import CLI

cli = CLI()
cli.parser.add_argument('-i','--input_file',required=True,help="Input Bam file")
WF = cli.parse_args()

dag_inputs = [ INPUT(output_path=cli.parsed_kwargs['input_file']) ]

class BamChunk(Tool):
    inputs = ['bam']
    outputs = ['dir']
    time_req = 0
    mem_req = 4000
    one_parent=True

    def cmd(self,i,t,s,p):
        return "python scripts/splitBam.py {t[input]} $OUT.dir"


####################
# Create DAG
####################

dag = (DAG()
       |Add| dag_inputs
       |Map| picard.REVERTSAM
       |Map| BamChunk
    )
dag.configure(settings=settings)
dag.add_to_workflow(WF)
WF.run()