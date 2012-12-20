__author__ = 'erik'

"""
Gunzip a directory
"""
from cosmos import session
from cosmos.Workflow.models import Workflow
from cosmos.contrib.ezflow.dag import DAG, Apply, Reduce, Split, ReduceSplit, Add
from cosmos.contrib.ezflow.tool import INPUT,Tool
import os
import pprint
class GUNZIP(Tool):
    inputs = ['gz']
    time_req = 60

    def cmd(self,i,t,s,p):
        return "gunzip {i[gz][0]}"


####################
# Create DAG
####################

indir = '/groups/lpm/erik/gatk/bundle2/b37'
#indir = '/cosmos/WGA/bundle/2.2/b37'

dag_inputs = [ INPUT(tags={'i':i+1},output_path=os.path.join(indir,p)) for i,p in enumerate(filter(lambda f: f[-3:] == '.gz', os.listdir(indir))) ]
dag = (DAG()
       |Add| dag_inputs
       |Apply| GUNZIP
    )

#################
# Run Workflow
#################

WF = Workflow.start('Gunzip GATK Bundle',restart=True)
dag.add_to_workflow(WF)
WF.run()
