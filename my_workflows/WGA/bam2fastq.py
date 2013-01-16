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
from cosmos.Workflow.cli import CLI


class GZIP(Tool):
    inputs = ['dir']
    time_req = 60
    one_parent = True

    def cmd(self,i,t,s,p):
        return "gzip -r {i[dir]}"

class SplitFastq(Tool):
    outputs = ['dir']
    time_req = 120
    mem_req = 1000
    one_parent=True

    def cmd(self,i,t,s,p):
        return "python scripts/splitfastq.py {t[input]} $OUT.dir"

####################
# Create DAG
####################

if os.environ['COSMOS_SETTINGS_MODULE'] == 'config.bioseq':
    indir = '/cosmos/WGA/bundle/2.2/b37/'
    dag_inputs = [ INPUT(tags={'i':i+1},output_path=os.path.join(indir,p)) for i,p in enumerate(filter(lambda f: f[-4:] == '.bam', os.listdir(indir))) ]

elif os.environ['COSMOS_SETTINGS_MODULE'] == 'config.orchestra':
    indir = '/groups/lpm/erik/WGA/ngs_data/CEU_WGS_Trio'
    dag_inputs = [ INPUT(output_path='/groups/lpm/erik/WGA/ngs_data/CEU_WGS_Trio/CEUTrio.HiSeq.WGS.b37_decoy.NA12878.clean.dedup.recal.20120117.bam') ]


dag = (DAG()
        |Add| dag_inputs
        |Apply| picard.REVERTSAM
        |Apply| picard.BAM2FASTQ
    )
dag.configure(settings=settings)

#################
# Run Workflow
#################

WF = CLI().parse_args()
dag.add_to_workflow(WF)
WF.run(finish=False)

#Split Fastqs
for input_tool in filter(lambda tool: tool.__class__.__name__ == 'BAM2FASTQ',dag.G.nodes()):
    input_dir = WF.stages.get(name='BAM2FASTQ').tasks.get(tags=input_tool.tags).output_files[0].path
    for fq in os.listdir(input_dir):
        dag.G.add_edge(input_tool,SplitFastq(dag=dag,tags={'input':os.path.join(input_dir,fq)}))

dag.add_to_workflow(WF)
WF.run()