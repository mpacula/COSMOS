__author__ = 'erik'

"""
Convert a Bam to Fastqs
"""
from cosmos import session
from cosmos.Workflow.models import Workflow
from cosmos.contrib.ezflow.dag import DAG, Apply, Reduce, Split, ReduceSplit, Add
from cosmos.contrib.ezflow.tool import INPUT,Tool
from tools import BamToFastq
import os
from settings import settings


####################
# Create DAG
####################

indir = '/groups/lpm/erik/WGA/ngs_data/CEU_WGS_Trio'

dag_inputs = [ INPUT(tags={'i':i+1},output_path=os.path.join(indir,p)) for i,p in enumerate(filter(lambda f: f[-4:] == '.bam', os.listdir(indir))) ]
dag = (DAG()
       |Add| dag_inputs
       |Apply| BamToFastq
    )
dag.configure(settings=settings)

#################
# Run Workflow
#################

WF = Workflow.start('CEU Trio BamToFastq',restart=True)
dag.add_to_workflow(WF)
WF.run()
