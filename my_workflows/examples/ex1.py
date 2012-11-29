import cosmos.session
from cosmos.Workflow.models import Workflow
from cosmos.contrib.ezflow.dag import DAG, Apply, Reduce, Split, ReduceSplit, Add
from tools import *

input_data = [
    #Sample, fq_chunk, fq_pair, output_path
    ('A',1,1,'/data/A_1_1.fastq'),
    ('A',1,2,'/data/A_1_2.fastq'),
 ]

####################
# Describe workflow
####################

# Tags
# Parameters
parameters = {
  'SAMPE': { 'q': 5 }
}

# Initialize
dag = DAG()

dag = (
       dag
       |Add| ([{'word':'hello'},{'word':'world'}],ECHO)
       |Split| ([('i',[1,2])],CAT)
       |Reduce| (['i'],PASTE)
    
)
dag.set_parameters(parameters)

#################
# Run Workflow
#################

WF = Workflow.start('test',restart=True)
dag.add_to_workflow(WF)
WF.run()
#print dag
dag.create_dag_img()

