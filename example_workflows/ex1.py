from cosmos.Workflow.models import Workflow
from cosmos.lib.ezflow.taskGraph import TaskGraph, add_,split_
from tools import ECHO, CAT

####################
# Workflow
####################

dag = TaskGraph().sequence_(
    add_([ ECHO(tags={'word':'hello'}), ECHO(tags={'word':'world'}) ]),
    split_([('x',[1,2])],CAT)
)
dag.create_dag_img('/tmp/ex.svg')

#################
# Run Workflow
#################

WF = Workflow.start('Example 1',restart=True)
dag.add_to_workflow(WF)
WF.run()
