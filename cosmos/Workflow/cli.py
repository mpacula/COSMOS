import argparse
import pprint
from models import Workflow

class CLI(object):

    def __init__(self):
        self.parser = argparse.ArgumentParser(description='Workflow CLI')
        self.parser.add_argument('-n','--name',help="A unique name for this workflow. All spaces are converted to underscores.",required=True)
        self.parser.add_argument('-r','--restart',action='store_true',help="Complete restart the workflow by deleting it and creating a new one. Optional.")
        self.parser.add_argument('-q','--default_queue',help="Deletes unsuccessful tasks in the workflow.")
        self.parser.add_argument('-d','--delete_intermediaries',action='store_true',help="Deletes intermediate files to save scratch space.")
        self.parser.add_argument('-dr','--dry_run',action='store_true',help="Don't actually run any jobs.")


    def parse_args(self):
        self.parsed_args = self.parser.parse_args()
        kwargs = dict(self.parsed_args._get_kwargs())
        self.kwargs = kwargs

        print kwargs
        wf = Workflow.start(**kwargs)
        wf.log.info('Starting working with args:\n{0}'.format(pprint.pprint(kwargs)))


