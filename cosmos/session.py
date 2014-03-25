#import sys

from cosmos.config import settings

#######################
# DJANGO
#######################

#configure django settings
#from cosmos import django_settings
#from django.conf import settings as django_conf_settings, global_settings

#custom template context processor for web interface
#django_conf_settings.configure(
#    TEMPLATE_CONTEXT_PROCESSORS=global_settings.TEMPLATE_CONTEXT_PROCESSORS + ('cosmos.utils.context_processor.contproc',),
#    **django_settings.__dict__)

# User can override this to specify the drmaa_native_specification method called at job submission
def default_drmaa_specification(jobAttempt):
    """
    Default method for the DRM specific resource usage flags passed in the jobtemplate's drmaa_native_specification.
    Can be overridden by the user when starting a workflow.

    :param jobAttempt: the jobAttempt being submitted
    """
    task = jobAttempt.task
    drm  = settings['DRM']

    cpu_req  = task.cpu_requirement
    mem_req  = task.memory_requirement
    time_req = task.time_requirement
    queue    = task.workflow.default_queue
    
    if drm == 'LSF': # for Orchestra Runs
        if time_req <= 12*60: queue = 'short'
        else:                 queue = 'long'
                
        return '-R "rusage[mem={0}] span[hosts=1]" -n {1} -W 0:{2} -q {3}'.format(mem_req, cpu_req, time_req, queue)

    elif drm == 'GE':
        return '-l spock_mem={mem_req}M,num_proc={cpu_req}'.format(mem_req=mem_req, cpu_req=cpu_req)

    else:
        raise Exception('DRM not supported')

#: The method to produce a :py:class:`cosmos.Job.models.JobAttempt`'s extra submission flags.
#: Can be overridden by user if special behavior is desired.
drmaa_spec = default_drmaa_specification

### print license info
warning = """
***********************************************************************************************************************
    Cosmos is currently NOT part of the public domain.  It is owned by and copyright Harvard Medical School
    and if you do not have permission to access Cosmos then the code and its documentation are all
    off limits and you are politely instructed to stop using Cosmos immediately and delete all files related to Cosmos.

    Thank you,
    Erik Gafni
    Harvard Medical School
    erik_gafni@hms.harvard.edu
***********************************************************************************************************************
"""
#printed_warning = False
#if not printed_warning:
#    print >> sys.stderr, warning
#    printed_warning=True
