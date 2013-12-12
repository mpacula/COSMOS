from cosmos.models.workflow.models import Workflow
from cosmos.flow.toolgraph import ToolGraph, split_,add_,map_,reduce_
from tools import ECHO, CAT, WC, PASTE

####################
# Workflow
####################

dag = ToolGraph().sequence_(
    add_([ ECHO(tags={'word':'hello'}), ECHO(tags={'word':'world'}) ]),
    split_([('i',[1,2])], CAT),
    reduce_([], PASTE),
    map_(WC)
)

dag.create_dag_img('/tmp/ex.svg')

#################
# Run Workflow
#################

WF = Workflow.start('Example 2',restart=True,delete_intermediates=True)
dag.add_to_workflow(WF)
WF.run()