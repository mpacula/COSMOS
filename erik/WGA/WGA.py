"""
WGA Workflow
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
cli.parser.add_argument('-i','--input_dir',type=str,help='Input directory')
WF = cli.parse_args()
#
#if os.environ['COSMOS_SETTINGS_MODULE'] == 'config.gpp':
#    indir = '/nas/erik/ngs_data/test_data3'
#elif os.environ['COSMOS_SETTINGS_MODULE'] == 'config.bioseq':
#    indir = '/cosmos/WGA/ngs_data/test_data'
#    indir = '/cosmos/WGA/ngs_data/NA12878_chr20'
#    wf_name = 'WGA NA12878_chr20'
#    inputs = get_inputs(indir)
#elif os.environ['COSMOS_SETTINGS_MODULE'] == 'config.orchestra':
#    indir = '/groups/lpm/erik/WGA/ngs_data/test_data'
#
#if 'inputs' not in locals():
#    if os.environ['COSMOS_SETTINGS_MODULE'] == 'config.default':
#        data_dict = [{u'lane': u'001', u'chunk': u'001', u'library': u'LIB-1216301779A', u'sample': u'1216301779A', u'platform': u'ILLUMINA', u'flowcell': u'C0MR3ACXX', u'pair': 0, u'path': u'/nas/erik/ngs_data/test_data3/Sample_1216301779A/1216301779A_GCCAAT_L001_R1_001.fastq.gz'}, {u'lane': u'001', u'chunk': u'001', u'library': u'LIB-1216301779A', u'sample': u'1216301779A', u'platform': u'ILLUMINA', u'flowcell': u'C0MR3ACXX', u'pair': 1, u'path': u'/nas/erik/ngs_data/test_data3/Sample_1216301779A/1216301779A_GCCAAT_L001_R2_001.fastq.gz'}, {u'lane': u'002', u'chunk': u'001', u'library': u'LIB-1216301779A', u'sample': u'1216301779A', u'platform': u'ILLUMINA', u'flowcell': u'C0MR3ACXX', u'pair': 0, u'path': u'/nas/erik/ngs_data/test_data3/Sample_1216301779A/1216301779A_GCCAAT_L002_R1_001.fastq.gz'}, {u'lane': u'002', u'chunk': u'001', u'library': u'LIB-1216301779A', u'sample': u'1216301779A', u'platform': u'ILLUMINA', u'flowcell': u'C0MR3ACXX', u'pair': 1, u'path': u'/nas/erik/ngs_data/test_data3/Sample_1216301779A/1216301779A_GCCAAT_L002_R2_001.fastq.gz'}, {u'lane': u'001', u'chunk': u'001', u'library': u'LIB-1216301781A', u'sample': u'1216301781A', u'platform': u'ILLUMINA', u'flowcell': u'C0MR3ACXX', u'pair': 0, u'path': u'/nas/erik/ngs_data/test_data3/Sample_1216301781A/1216301781A_CTTGTA_L001_R1_001.fastq.gz'}, {u'lane': u'001', u'chunk': u'001', u'library': u'LIB-1216301781A', u'sample': u'1216301781A', u'platform': u'ILLUMINA', u'flowcell': u'C0MR3ACXX', u'pair': 1, u'path': u'/nas/erik/ngs_data/test_data3/Sample_1216301781A/1216301781A_CTTGTA_L001_R2_001.fastq.gz'}, {u'lane': u'002', u'chunk': u'001', u'library': u'LIB-1216301781A', u'sample': u'1216301781A', u'platform': u'ILLUMINA', u'flowcell': u'C0MR3ACXX', u'pair': 0, u'path': u'/nas/erik/ngs_data/test_data3/Sample_1216301781A/1216301781A_CTTGTA_L002_R1_001.fastq.gz'}, {u'lane': u'002', u'chunk': u'001', u'library': u'LIB-1216301781A', u'sample': u'1216301781A', u'platform': u'ILLUMINA', u'flowcell': u'C0MR3ACXX', u'pair': 1, u'path': u'/nas/erik/ngs_data/test_data3/Sample_1216301781A/1216301781A_CTTGTA_L002_R2_001.fastq.gz'}]
#        wf_name = 'Test'
#    else:
#        from my_workflows.WGA.inputs import batch1
#        data_dict = json.loads(batch1.main(input_dir=indir,depth=1))
#        wf_name = 'Test'
#
inputs = []
for i in get_inputs(cli.parsed_kwargs['input_dir']):
    path = i.pop('path')
    inputs.append(INPUT(tags = i,taskfile=TaskFile(path=path,fmt='fastq.gz')))


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
    |Reduce| (['sample'],picard.MERGE_SAMS)
    |Apply| picard.CLEAN_SAM
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