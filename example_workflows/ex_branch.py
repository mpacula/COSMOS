"""
This workflow demonstrates branching for when you need
something more complicated than a linear step-by-step
series of stages.

cosmos.lib.ezflow.dag.DAG.branch() is the key to branching.
"""

from cosmos.Workflow.models import Workflow
from cosmos.lib.ezflow.taskGraph import TaskGraph
import tools

####################
# Workflow
####################

dag = ( TaskGraph()
          .add([ tools.ECHO(tags={'word':'hello'}), tools.ECHO(tags={'word':'world'}) ])
          .split([('i',[1,2])],tools.CAT)
          .map(tools.WC)
        .branch('ECHO')
          .map(tools.WC,'Extra Independent Word Count')
)

# Generate image
dag.create_dag_img('/tmp/ex_branch.svg')

#################
# Run Workflow
#################

WF = Workflow.start('Example Branch',restart=True)
dag.add_to_workflow(WF)
WF.run()