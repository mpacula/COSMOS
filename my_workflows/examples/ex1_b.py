from cosmos import session
from cosmos.Workflow.models import Workflow
from cosmos.contrib.ezflow.dag import DAG, Apply, Reduce, Split, ReduceSplit, Add
from tools import ECHO, CAT, PASTE, WC

####################
# Workflow
####################

dag = ( DAG()
    |Add| [ ECHO(tags={'word':'hello'}), ECHO(tags={'word':'world'}) ]
    |Split| ([('i',[1,2])],CAT)
    
)
dag.create_dag_img('/tmp/ex1.svg')

#################
# Run Workflow
#################

# restart changed to False.  If True then all successful tasks will be deleted and re-executed.
WF = Workflow.start('Example 1',restart=False) 
dag.add_to_workflow(WF)
WF.run()