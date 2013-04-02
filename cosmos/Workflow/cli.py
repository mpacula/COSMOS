import argparse
import pprint
from models import Workflow
import sys

class CLI(object):

    def __init__(self):
        self.parser = argparse.ArgumentParser(description='Workflow CLI')
        CLI.add_default_args(self.parser)

    @staticmethod
    def add_default_args(parser):
        parser.add_argument('-n','--name',help="A unique name for this workflow. All spaces are converted to underscores.",required=True)
        parser.add_argument('-q','--default_queue',type=str,help="Deletes unsuccessful tasks in the workflow.  Defaults to the value in cosmos.session.settings.")
        parser.add_argument('-o','--root_output_dir',type=str,help="The root output directory.  Output will be stored in root_output_dir/{workflow.name}.  Defaults to the value in cosmos.session.settings.")
        parser.add_argument('-r','--restart',action='store_true',help="Complete restart the workflow by deleting it and creating a new one.")
        parser.add_argument('-di','--delete_intermediates',action='store_true',help="Deletes intermediate files to save scratch space.")
        parser.add_argument('-ds','--delete_unsuccessful_stages',action='store_true',help="If reloading, deletes any stages that are unsuccessful")
        parser.add_argument('-y','--prompt_confirm',action='store_false',help="Do not use confirmation prompts before restarting or deleting, and assume answer is always yes.")
        parser.add_argument('-dry','--dry_run',action='store_true',help="Don't actually run any jobs.  Experimental.")

    def parse_args(self):
        """
        runs the argument parser

        :param margs: arguments to set manually
        :returns: a workflow instance
        """
        self.parsed_args = self.parser.parse_args()
        kwargs = dict(self.parsed_args._get_kwargs())
        self.parsed_kwargs = kwargs
        wf = Workflow.start(**kwargs)
        wf.log.info('Args:\n{0}'.format(pprint.pformat(kwargs)))
        return wf


