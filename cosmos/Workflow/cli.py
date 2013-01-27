import argparse
import pprint
from models import Workflow

class CLI(object):

    def __init__(self):
        self.parser = argparse.ArgumentParser(description='Workflow CLI')
        self.parser.add_argument('-n','--name',help="A unique name for this workflow. All spaces are converted to underscores.",required=True)
        self.parser.add_argument('-q','--default_queue',type=str,help="Deletes unsuccessful tasks in the workflow.  Defaults to the value in cosmos.session.settings.")
        self.parser.add_argument('-o','--root_output_dir',type=str,help="The root output directory.  Output will be stored in root_output_dir/{workflow.name}.  Defaults to the value in cosmos.session.settings.")
        self.parser.add_argument('-r','--restart',action='store_true',help="Complete restart the workflow by deleting it and creating a new one.")
        self.parser.add_argument('-d','--delete_intermediaries',action='store_true',help="Deletes intermediate files to save scratch space.")
        self.parser.add_argument('-y','--prompt_confirm',action='store_false',help="Do not use confirmation prompts before restarting or deleting, and assume answer is always yes.")
        self.parser.add_argument('-dry','--dry_run',action='store_true',help="Don't actually run any jobs.")

    def parse_args(self):
        """
        runs the argument parser
        """
        self.parsed_args = self.parser.parse_args()
        kwargs = dict(self.parsed_args._get_kwargs())
        self.parsed_kwargs = kwargs
        wf = Workflow.start(**kwargs)
        wf.log.info('Args:\n{0}'.format(pprint.pformat(kwargs)))
        return wf


