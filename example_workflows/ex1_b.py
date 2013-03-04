from cosmos.Workflow.models import Workflow
from cosmos.contrib.ezflow.dag import DAG, Split, Add, Map
from tools import ECHO, CAT, WC

####################
# Workflow
####################

dag = ( DAG()
    |Add| [ ECHO(tags={'word':'hello'}), ECHO(tags={'word':'world'}) ]
    |Split| ([('i',[1,2])],CAT)
    |Map| WC

)
dag.create_dag_img('/tmp/ex1.svg')
#dag.configure(parameters={'WC':' -p'})

#################
# Run Workflow
#################

WF = Workflow.start('Example 1')
dag.add_to_workflow(WF)
WF.run()