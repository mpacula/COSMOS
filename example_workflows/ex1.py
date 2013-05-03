from cosmos.Workflow.models import Workflow
from cosmos.contrib.ezflow.dag import DAG, add_,split_,sequence_
from tools import ECHO, CAT, PASTE, WC

####################
# Workflow
####################

dag = DAG().sequence_(
    add_([ ECHO(tags={'word':'hello'}), ECHO(tags={'word':'world'}) ]),
    split_([('i',[1,2])],CAT)
)
dag.create_dag_img('/tmp/ex.svg')

#################
# Run Workflow
#################

WF = Workflow.start('Example Fail',restart=True)
dag.add_to_workflow(WF)
WF.run()
