from cosmos.Workflow.models import Workflow
from cosmos.contrib.ezflow.dag import DAG, Split, Add
from tools import ECHO, CAT, PASTE, WC

####################
# Workflow
####################

dag = ( DAG()
    |Add| [ ECHO(tags={'word':'hello'}), ECHO(tags={'word':'world'}) ]
    |Split| ([('i',[1,2])],CAT)
)
dag.create_dag_img('/tmp/ex.svg')

import ipdb; ipdb.set_trace()

#################
# Run Workflow
#################

WF = Workflow.start('Example 1',restart=True)
dag.add_to_workflow(WF)
WF.run()