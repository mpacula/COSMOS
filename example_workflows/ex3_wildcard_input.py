from cosmos.Workflow.models import Workflow
from cosmos.contrib.ezflow.dag import DAG, Split, Add, Map, Reduce
from tools import ECHO, CAT, WC, MD5Sum
from cosmos.Workflow.cli import CLI

cli = CLI()
WF = cli.parse_args() # parses command line arguments

####################
# Workflow
####################

dag = ( DAG()
    |Add| [ ECHO(tags={'word':'hello'}), ECHO(tags={'word':'world'}) ]
    |Reduce| ([],MD5Sum)
)

dag.create_dag_img('/tmp/ex1.svg')
#dag.configure(parameters={'WC':' -p'})

#################
# Run Workflow
#################

dag.add_to_workflow(WF)
WF.run()