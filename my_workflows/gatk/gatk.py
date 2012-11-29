from cosmos import session
from cosmos.Workflow.models import Workflow
from cosmos.contrib.ezflow.dag import DAG, Apply, Reduce, Split, ReduceSplit, Add
from cosmos.contrib.ezflow.tool import INPUT
from tools import *
import os

input_data = [
    #Sample, lane, fq_chunk, fq_pair, fq_path, RG_ID, RG_LIB, RG_PLATFORM
    ('A',4,1,1,'/data/A_1_1.fastq','rgid','lib','illumina'),
    ('A',4,1,2,'/data/A_1_2.fastq','rgid','lib','illumina'),
    ('A',4,2,1,'/data/A_2_1.fastq','rgid','lib','illumina'),
    ('A',4,2,2,'/data/A_2_2.fastq','rgid','lib','illumina'),
    ('B',4,1,1,'/data/B_1_1.fastq','rgid','lib','illumina'),
    ('B',4,1,2,'/data/B_1_2.fastq','rgid','lib','illumina'),
 ]


####################
# Describe workflow
####################

#Configuration
resource_bundle_path = '/nas/erik/bundle/1.5/b37'
settings = {
    'GATK_path' : '/home/ch158749/tools/GenomeAnalysisTKLite-2.1-8-gbb7f038',
    'Picard_path' : '/home/ch158749/tools/picard-tools-1.77',
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

parameters = {
  'SAMPE': { 'q': 5 },
  'MERGE_SAMS' : { 'assume_sorted' : True }
}

# Tags
intervals = ('interval',[3,4])
glm = ('glm',['SNP','INDEL'])
dbs = ('database',['1000G','PolyPhen2','COSMIC','ENCODE'])

# Initialize
dag = DAG()
inputs = [ INPUT(tags={'sample':x[0],'lane':x[1],'fq_chunk':x[2],'fq_pair':x[3],'RG_ID':x[4], 'RG_LIB':x[5], 'RG_PLATFORM':x[6]},filepaths=[x[4]]) for x in input_data ]

dag = (
    dag
    |Add| inputs
    |Apply| ALN
    |Reduce| (['sample','lane','fq_chunk'],SAMPE)
#    |Reduce| (['sample'],MERGE_SAMS)
#    |Apply| (CLEAN_SAM)
#    |Split| ([intervals],RTC)
#    |Apply| IR
#    |Reduce| (['sample'],BQSR)
#    |Apply| PR
#    |ReduceSplit| ([],[glm,intervals], UG)
#    |Reduce| (['glm'],CV)
#    |Apply| VQSR
#    |Apply| Apply_VQSR
#    |Reduce| ([],CV,"CV 2")
#    |Split| ([dbs],ANNOVAR)
#    |Apply| PROCESS_ANNOVAR
#    |Reduce| ([],MERGE_ANNOTATIONS)
#    |Apply| SQL_DUMP
#    |Apply| ANALYSIS
)
dag.configure(settings,parameters)

#################
# Run Workflow
#################

WF = Workflow.start('test',restart=True)
dag.create_dag_img('/tmp/graph.svg')
dag.add_to_workflow(WF)

#print dag

