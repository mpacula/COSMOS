"""
WGA Workflow
"""
_author_ = 'Erik Gafni'

from cosmos import session
from cosmos.Workflow.models import Workflow
from cosmos.contrib.ezflow.dag import DAG, Apply, Reduce, Split, ReduceSplit, Add
from cosmos.contrib.ezflow.tool import INPUT
import make_data_dict
from tools import *
import json
from settings import settings

####################
# Input
####################


if os.environ['COSMOS_SETTINGS_MODULE'] == 'config.gpp':
    indir = '/nas/erik/ngs_data/test_data3'
elif os.environ['COSMOS_SETTINGS_MODULE'] == 'config.bioseq':
    indir = '/cosmos/WGA/ngs_data/test_data'
elif os.environ['COSMOS_SETTINGS_MODULE'] == 'config.orchestra':
    indir = '/groups/lpm/erik/WGA/ngs_data/test_data'

if 'COSMOS_SETTINGS_MODULE' not in os.environ:
    data_dict = [{u'lane': u'001', u'chunk': u'001', u'library': u'LIB-1216301779A', u'sample': u'1216301779A', u'platform': u'ILLUMINA', u'flowcell': u'C0MR3ACXX', u'pair': 0, u'path': u'/nas/erik/ngs_data/test_data3/Sample_1216301779A/1216301779A_GCCAAT_L001_R1_001.fastq'}, {u'lane': u'001', u'chunk': u'001', u'library': u'LIB-1216301779A', u'sample': u'1216301779A', u'platform': u'ILLUMINA', u'flowcell': u'C0MR3ACXX', u'pair': 1, u'path': u'/nas/erik/ngs_data/test_data3/Sample_1216301779A/1216301779A_GCCAAT_L001_R2_001.fastq'}, {u'lane': u'002', u'chunk': u'001', u'library': u'LIB-1216301779A', u'sample': u'1216301779A', u'platform': u'ILLUMINA', u'flowcell': u'C0MR3ACXX', u'pair': 0, u'path': u'/nas/erik/ngs_data/test_data3/Sample_1216301779A/1216301779A_GCCAAT_L002_R1_001.fastq'}, {u'lane': u'002', u'chunk': u'001', u'library': u'LIB-1216301779A', u'sample': u'1216301779A', u'platform': u'ILLUMINA', u'flowcell': u'C0MR3ACXX', u'pair': 1, u'path': u'/nas/erik/ngs_data/test_data3/Sample_1216301779A/1216301779A_GCCAAT_L002_R2_001.fastq'}, {u'lane': u'001', u'chunk': u'001', u'library': u'LIB-1216301781A', u'sample': u'1216301781A', u'platform': u'ILLUMINA', u'flowcell': u'C0MR3ACXX', u'pair': 0, u'path': u'/nas/erik/ngs_data/test_data3/Sample_1216301781A/1216301781A_CTTGTA_L001_R1_001.fastq'}, {u'lane': u'001', u'chunk': u'001', u'library': u'LIB-1216301781A', u'sample': u'1216301781A', u'platform': u'ILLUMINA', u'flowcell': u'C0MR3ACXX', u'pair': 1, u'path': u'/nas/erik/ngs_data/test_data3/Sample_1216301781A/1216301781A_CTTGTA_L001_R2_001.fastq'}, {u'lane': u'002', u'chunk': u'001', u'library': u'LIB-1216301781A', u'sample': u'1216301781A', u'platform': u'ILLUMINA', u'flowcell': u'C0MR3ACXX', u'pair': 0, u'path': u'/nas/erik/ngs_data/test_data3/Sample_1216301781A/1216301781A_CTTGTA_L002_R1_001.fastq'}, {u'lane': u'002', u'chunk': u'001', u'library': u'LIB-1216301781A', u'sample': u'1216301781A', u'platform': u'ILLUMINA', u'flowcell': u'C0MR3ACXX', u'pair': 1, u'path': u'/nas/erik/ngs_data/test_data3/Sample_1216301781A/1216301781A_CTTGTA_L002_R2_001.fastq'}]
else:
    data_dict = json.loads(make_data_dict.main(input_dir=indir,depth=1))

inputs = []
for i in data_dict:
    path = i.pop('path')
    inputs.append(INPUT(tags = i,output_paths=[path]))


####################
# Configuration
####################




#parameter keywords can be the name of the tool class, or its default __verbose__
parameters = {
  'ALN': { 'q': 5 },
}

# Tags
intervals = ('interval',[2,3])
glm = ('glm',['SNP','INDEL'])
dbs = ('database',['1000G','PolyPhen2','COSMIC','ENCODE'])

####################
# Create DAG
####################

dag = (DAG()
    |Add| inputs
    |Apply| ALN
    |Reduce| (['sample','lane','chunk'],SAMPE)
    |Reduce| (['sample'],MERGE_SAMS)
    |Apply| CLEAN_SAM
    |Apply| INDEX_BAM
    |Split| ([intervals],RTC)
    |Apply| IR
    |Reduce| (['sample'],BQSR)
    |Apply| PR
    |ReduceSplit| ([],[glm,intervals], UG)
    |Reduce| (['glm'],CV)
    |Apply| VQSR
    |Apply| Apply_VQSR
    |Reduce| ([],CV,"CV 2")
#    |Split| ([dbs],ANNOVAR)
#    |Apply| PROCESS_ANNOVAR
#    |Reduce| ([],MERGE_ANNOTATIONS)
#    |Apply| SQL_DUMP
#    |Apply| ANALYSIS
)
dag.configure(settings,parameters)
dag.create_dag_img('/tmp/graph.svg')

#################
# Run Workflow
#################

WF = Workflow.start('test',restart=False)
dag.add_to_workflow(WF)
WF.run()
