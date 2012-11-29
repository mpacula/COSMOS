import cosmos.session
from cosmos.Workflow.models import Workflow
from dag import DAG, Apply, Reduce, Split, ReduceSplit
from gatk_tools import *
from cosmos.Workflow.models import TaskFile
from tool import INPUT

input_data = [
    #Sample, fq_chunk, fq_pair, output_path
    ('A',1,1,'/data/A_1_1.fastq'),
    ('A',1,2,'/data/A_1_2.fastq'),
    ('A',2,1,'/data/A_2_1.fastq'),
    ('A',2,2,'/data/A_2_2.fastq'),
    ('B',1,1,'/data/B_1_1.fastq'),
    ('B',1,2,'/data/B_1_2.fastq'),
 ]

####################
# Describe workflow
####################

# Tags
intervals = ('interval',[3,4])
glm = ('glm',['SNP','INDEL'])
dbs = ('database',['1000G','PolyPhen2','COSMIC','ENCODE'])

# Parameters
parameters = {
  'SAMPE': { 'q': 5 }
}

# Initialize
dag = DAG()
for x in input_data:
    dag.add_input(tags={'sample':x[0],'fq_chunk':x[1],'fq_pair':x[2]},filepaths=[x[3]])

dag = (
    dag
    |Apply| ALN
    |Reduce| (['sample','fq_chunk'],SAMPE)
    |Reduce| (['sample'],CLEAN_SAM)
    |Split| ([intervals],IRTC)
    |Apply| IR
    |Reduce| (['sample'],BQSR)
    |Apply| PR
    |ReduceSplit| ([],[glm,intervals], UG)
    |Reduce| (['glm'],CV)
    |Apply| VQSR
    |Apply| Apply_VQSR
    |Reduce| ([],CV,"CV 2")
    |Split| ([dbs],ANNOVAR)
    |Apply| PROCESS_ANNOVAR
    |Reduce| ([],MERGE_ANNOTATIONS)
    |Apply| SQL_DUMP
    |Apply| ANALYSIS
)
dag.set_parameters(parameters)

#################
# Run Workflow
#################

WF = Workflow.start('test',restart=True)
dag.add_to_workflow(WF)

#print dag
dag.create_dag_img()

