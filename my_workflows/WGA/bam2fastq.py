__author__ = 'erik'

"""
Convert a Bam to Fastqs
"""
from cosmos import session
from cosmos.Workflow.models import Workflow
from cosmos.contrib.ezflow.dag import DAG, Apply, Reduce, Split, ReduceSplit, Add
from cosmos.contrib.ezflow.tool import INPUT,Tool
from tools import picard
import os
from settings import settings


class GZIP(Tool):
    inputs = ['dir']
    time_req = 60

    def cmd(self,i,t,s,p):
        return "gzip -r {i[dir]}"


####################
# Create DAG
####################


if os.environ['COSMOS_SETTINGS_MODULE'] == 'config.bioseq':
    indir = '/cosmos/WGA/bundle/2.2/b37/'
    WF = Workflow.start('BamToFastq NA12878 Chr20',restart=False)
elif os.environ['COSMOS_SETTINGS_MODULE'] == 'config.orchestra':
    indir = '/groups/lpm/erik/WGA/ngs_data/CEU_WGS_Trio'
    WF = Workflow.start('CEU Trio BamToFastq',restart=False)


dag_inputs = [ INPUT(tags={'i':i+1},output_path=os.path.join(indir,p)) for i,p in enumerate(filter(lambda f: f[-4:] == '.bam', os.listdir(indir))) ]
dag = (DAG()
        |Add| dag_inputs
        |Apply| picard.BAM2FASTQ
        |Apply| GZIP
    )
dag.configure(settings=settings)

#################
# Run Workflow
#################

dag.add_to_workflow(WF)
WF.run()
