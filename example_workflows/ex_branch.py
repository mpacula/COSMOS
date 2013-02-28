"""
This workflow demonstrates branching for when you need
something more complicated than a linear step-by-step
series of stages.

cosmos.contrib.ezflow.dag.DAG.branch() is the key to branching.
"""

from cosmos.Workflow.models import Workflow
from cosmos.contrib.ezflow.dag import DAG, Workflow, Split, Add
import tools

####################
# Workflow
####################

dag = ( DAG()
        |Add| [ tools.ECHO(tags={'word':'hello'}), tools.ECHO(tags={'word':'world'}) ]
        |Split| ([('i',[1,2])],tools.CAT)
        |Workflow| tools.WC

)
# Add an independent Word Count Job, who's stage's name will be "Extra Word Count"
dag.branch('ECHO') |Workflow| (tools.WC,'Extra Independent Word Count')

# Generate image
dag.create_dag_img('/tmp/ex_branch.svg')

#################
# Run Workflow
#################

WF = Workflow.start('Example 2',restart=True)
dag.add_to_workflow(WF)
WF.run()