from cosmos.Workflow.models import Workflow
from cosmos.contrib.ezflow.dag import DAG, Apply, Split, Add
from tools import ECHO, CAT, WC

####################
# Workflow
####################

dag = ( DAG()
    |Add| [ ECHO(tags={'word':'hello'}), ECHO(tags={'word':'world'}) ]
    |Split| ([('i',[1,2])],CAT)
    |Apply| WC

)
dag.create_dag_img('/tmp/ex1.svg')
#dag.configure(parameters={'WC':' -p'})

#################
# Run Workflow
#################

# restart changed to False.  If True then all successful tasks will be deleted and re-executed.
WF = Workflow.start('Example 1',restart=True,delete_intermediaries=True)
dag.add_to_workflow(WF)
WF.run()