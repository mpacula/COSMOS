"""
A Cosmos session.  Must be imported at the top of a cosmos script, after cosmos.config.configure() has been called
sometime in the interpreted session
"""
import sys
import config
import os
import shutil
from cosmos.utils.helpers import confirm

if config.settings is None:
    if os.path.exists(config.config_path):
        config.configure_from_file(config.config_path)
    else:
        if not os.path.exists(config.config_path):
            msg = 'No configuration file exists (and cosmos has not been configured manually),' \
                  'would you like to create a default one in {0}?'.format(config.config_path)
            if confirm(msg, default=True):
                if not os.path.exists(os.path.dirname(config.config_path)):
                    os.mkdir(os.path.dirname(config.config_path))
                shutil.copyfile(config.default_config_path, config.config_path)
                print >> sys.stderr, "Done.  Before proceeding, please edit {0}".format(config.config_path)
            else:
                sys.exit(1)
        # raise Exception('Cannot import session until Cosmos has been configured by calling cosmos.config.configure(), or'
        #                 'setting up a configuration file in ~/.cosmos/config.ini')

settings = config.settings

#######################
# DJANGO
#######################

def default_get_drmaa_native_specification(jobAttempt):
    """
    Default method for the DRM specific resource usage flags passed in the jobtemplate's drmaa_native_specification.
    Can be overridden by the user when starting a workflow.

    :param jobAttempt: the jobAttempt being submitted
    """
    task = jobAttempt.task
    DRM = config.settings['DRM']

    cpu_req = task.cpu_requirement
    mem_req = task.memory_requirement
    time_req = task.time_requirement
    queue = task.workflow.default_queue

    if 'LSF' in DRM:
        s = '-R "rusage[mem={0}] span[hosts=1]" -n {1}'.format(mem_req/cpu_req,cpu_req)
        if time_req:
            s += ' -W 0:{0}'.format(time_req)
        if queue:
            s += ' -q {0}'.format(queue)
        return s
    elif 'GE' in DRM:
        return '-l h_vmem={mem_req}M,num_proc={cpu_req}'.format(
            mem_req=mem_req,
            cpu_req=cpu_req)
    else:
        raise Exception('DRM not supported')

#: The method to produce a :py:class:`cosmos.Job.models.JobAttempt`'s extra submission flags.
#: Can be overridden by user if special behavior is desired.
get_drmaa_native_specification = default_get_drmaa_native_specification

### print license info
warning = """
***********************************************************************************************************************
    Cosmos is currently NOT part of the public domain.  It is owned by and copywrite Harvard Medical School
    and if you do not have permission to access Cosmos then the code and its documentation are all
    off limits and you are politely instructed to stop using Cosmos immediately and delete all files related to Cosmos.

    Thank you,
    Erik Gafni
    Harvard Medical School
    erik_gafni@hms.harvard.edu
***********************************************************************************************************************
"""
printed_warning = False
if config.settings['license_warning'] != 'False':
    if not printed_warning:
        print >> sys.stderr, warning
        printed_warning=True