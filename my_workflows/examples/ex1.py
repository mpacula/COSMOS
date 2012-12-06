from cosmos import session
from cosmos.Workflow.models import Workflow
from cosmos.contrib.ezflow.dag import DAG, Apply, Reduce, Split, ReduceSplit, Add
from tools import ECHO, CAT, PASTE, WC

####################
# Workflow
####################

dag = DAG()
dag = ( dag
    |Add| [ ECHO(tags={'word':'hello'}), ECHO(tags={'word':'world'}) ]
    |Split| ([('i',[1,2])],CAT)
)
dag.create_dag_img('/tmp/ex1.svg')

#################
# Run Workflow
#################

WF = Workflow.start('Example 1',restart=True)
dag.add_to_workflow(WF)
WF.run()