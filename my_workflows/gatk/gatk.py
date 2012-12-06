from cosmos import session
from cosmos.Workflow.models import Workflow
from cosmos.contrib.ezflow.dag import DAG, Apply, Reduce, Split, ReduceSplit, Add
from cosmos.contrib.ezflow.tool import INPUT
import make_data_dict
from tools import *
import json

if os.environ['COSMOS_SETTINGS_MODULE'] == 'config.gpp':
    data_dict = json.loads(make_data_dict.main(input_dir='/nas/erik/ngs_data/test_data3',depth=1))

data_dict = [{u'lane': u'001', u'chunk': u'001', u'library': u'LIB-1216301779A', u'sample': u'1216301779A', u'platform': u'ILLUMINA', u'flowcell': u'C0MR3ACXX', u'pair': 0, u'path': u'/nas/erik/ngs_data/test_data3/Sample_1216301779A/1216301779A_GCCAAT_L001_R1_001.fastq'}, {u'lane': u'001', u'chunk': u'001', u'library': u'LIB-1216301779A', u'sample': u'1216301779A', u'platform': u'ILLUMINA', u'flowcell': u'C0MR3ACXX', u'pair': 1, u'path': u'/nas/erik/ngs_data/test_data3/Sample_1216301779A/1216301779A_GCCAAT_L001_R2_001.fastq'}, {u'lane': u'002', u'chunk': u'001', u'library': u'LIB-1216301779A', u'sample': u'1216301779A', u'platform': u'ILLUMINA', u'flowcell': u'C0MR3ACXX', u'pair': 0, u'path': u'/nas/erik/ngs_data/test_data3/Sample_1216301779A/1216301779A_GCCAAT_L002_R1_001.fastq'}, {u'lane': u'002', u'chunk': u'001', u'library': u'LIB-1216301779A', u'sample': u'1216301779A', u'platform': u'ILLUMINA', u'flowcell': u'C0MR3ACXX', u'pair': 1, u'path': u'/nas/erik/ngs_data/test_data3/Sample_1216301779A/1216301779A_GCCAAT_L002_R2_001.fastq'}, {u'lane': u'001', u'chunk': u'001', u'library': u'LIB-1216301781A', u'sample': u'1216301781A', u'platform': u'ILLUMINA', u'flowcell': u'C0MR3ACXX', u'pair': 0, u'path': u'/nas/erik/ngs_data/test_data3/Sample_1216301781A/1216301781A_CTTGTA_L001_R1_001.fastq'}, {u'lane': u'001', u'chunk': u'001', u'library': u'LIB-1216301781A', u'sample': u'1216301781A', u'platform': u'ILLUMINA', u'flowcell': u'C0MR3ACXX', u'pair': 1, u'path': u'/nas/erik/ngs_data/test_data3/Sample_1216301781A/1216301781A_CTTGTA_L001_R2_001.fastq'}, {u'lane': u'002', u'chunk': u'001', u'library': u'LIB-1216301781A', u'sample': u'1216301781A', u'platform': u'ILLUMINA', u'flowcell': u'C0MR3ACXX', u'pair': 0, u'path': u'/nas/erik/ngs_data/test_data3/Sample_1216301781A/1216301781A_CTTGTA_L002_R1_001.fastq'}, {u'lane': u'002', u'chunk': u'001', u'library': u'LIB-1216301781A', u'sample': u'1216301781A', u'platform': u'ILLUMINA', u'flowcell': u'C0MR3ACXX', u'pair': 1, u'path': u'/nas/erik/ngs_data/test_data3/Sample_1216301781A/1216301781A_CTTGTA_L002_R2_001.fastq'}]
    
inputs = []
for i in data_dict:
    path = i.pop('path')
    inputs.append(INPUT(tags = i,output_paths=[path]))


####################
# Describe workflow
####################

#Configuration
resource_bundle_path = '/nas/erik/bundle/1.5/b37'
settings = {
    'GATK_path' : '/home/ch158749/tools/GenomeAnalysisTKLite-2.1-8-gbb7f038/GenomeAnalysisTKLite.jar',
    'Picard_dir' : '/home/ch158749/tools/picard-tools-1.77',
    'bwa_path' : '/home/ch158749/tools/bwa-0.6.2/bwa',
    'resource_bundle_path' : resource_bundle_path,
    'bwa_reference_fasta_path' : '/nas/erik/bwa_reference/human_g1k_v37.fasta',
    'tmp_dir' : session.settings.tmp_dir,
    'reference_fasta_path' : os.path.join(resource_bundle_path,'human_g1k_v37.fasta'),
    'dbsnp_path' : os.path.join(resource_bundle_path,'dbsnp_135.b37.vcf'),
    'hapmap_path' : os.path.join(resource_bundle_path,'hapmap_3.3.b37.sites.vcf'),
    'omni_path' : os.path.join(resource_bundle_path,'1000G_omni2.5.b37.sites.vcf'),
    'mills_path' : os.path.join(resource_bundle_path,'Mills_and_1000G_gold_standard.indels.b37.vcf'),
    'indels_1000g_phase1_path' : os.path.join(resource_bundle_path,'1000G_phase1.indels.b37.vcf')
}

#parameter keywords can be the name of the tool class, or its default __verbose__
parameters = {
  'ALN': { 'q': 5 },
}

# Tags
intervals = ('interval',[3,4])
glm = ('glm',['SNP','INDEL'])
dbs = ('database',['1000G','PolyPhen2','COSMIC','ENCODE'])

# Describe Workflow
dag = DAG()
dag = ( dag
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
