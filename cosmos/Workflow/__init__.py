import argparse
from cosmos.Workflow.models import Workflow

def cli():
    import argparse

    parser = argparse.ArgumentParser(description='Workflow CLI')
    parser.add_argument('-n','--name',help="A unique name for this workflow. All spaces are converted to underscores.",required=True)
    parser.add_argument('-r','--restart',action='store_true',help="Complete restart the workflow by deleting it and creating a new one. Optional.")
    parser.add_argument('-q','--default_queue',help="Deletes unsuccessful tasks in the workflow.")
    parser.add_argument('-d','--delete_intermediaries',action='store_true',help="Deletes intermediate files to save scratch space.")

    args = parser.parse_args()
    kwargs = dict(args._get_kwargs())
    kwargs['dry_run'] = False
    return Workflow.start(**kwargs)