import pprint
import sys
import os

from cosmos.models import Workflow
from cosmos.session import settings


def add_workflow_args(parser):
    parser.add_argument('-n', '--name',
                        help="A name for this workflow", required=True)
    parser.add_argument('-q', '--default_queue', type=str,
                        help="Default queue.  Defaults to the value in cosmos.session.settings.")
    parser.add_argument('-o', '--root_output_dir', type=str, default=settings['default_root_output_dir'],
                        help="The root output directory.  Output will be stored in root_output_dir/{workflow.name}.  "
                             "Defaults to the value in cosmos.session.settings.")
    parser.add_argument('-c', '--max_cores', type=int,
                        help="Maximum number (based on the sum of cpu_requirement) of cores to use at once.  0 means"
                             "unlimited", default=0)
    parser.add_argument('-r', '--restart', action='store_true',
                        help="Completely restart the workflow.  Note this will delete all records and output files of"
                             "the workflow being restarted!")
    parser.add_argument('-di', '--delete_intermediates', action='store_true',
                        help="Deletes intermediate files to save scratch space.")
    parser.add_argument('-y', '--prompt_confirm', action='store_false',
                        help="Do not use confirmation prompts before restarting or deleting, and assume answer is always yes.")
    parser.add_argument('-dry', '--dry_run', action='store_true', help="Don't actually run any jobs.  Experimental.")


def parse_args(parser):
    """
    Runs the argument parser

    :returns: a workflow instance and kwargs parsed by argparse
    """
    parsed_args = parser.parse_args()
    kwargs = dict(parsed_args._get_kwargs())

    #extract wf_kwargs from kwargs
    wf_kwargs = dict([(k, kwargs[k]) for k
                      in
                      ['name', 'default_queue', 'root_output_dir', 'restart', 'delete_intermediates', 'prompt_confirm',
                       'dry_run', 'max_cores']])
    cmd_args = [a if ' ' not in a else "'" + a + "'" for a in sys.argv[1:]]
    wf_kwargs['comments'] = '$ ' + ' '.join([os.path.basename(sys.argv[0])] + cmd_args)

    wf = Workflow.start(**wf_kwargs)

    wf.log.info('Parsed kwargs:\n{0}'.format(pprint.pformat(kwargs)))
    return wf, kwargs