from subprocess import Popen
import os

from cosmos.models.job.JobAttempt import JobAttempt
from cosmos.models.job.jobmanager import JobManagerBase


class JobStatusError(Exception):
    pass

all_processes = {}
current_processes = {}

def preexec_function():
    # Ignore the SIGINT signal by setting the handler to the standard
    # signal handler SIG_IGN.  This allows Cosmos to cleanly
    # terminate jobs when there is a ctrl+c event
    os.setpgrp()

class JobManager(JobManagerBase):
    """
    Note there can only be one of these instantiated at a time
    """
    class Meta:
        app_label = 'cosmos'
        db_table = 'cosmos_jobmanager'

    def _submit_job(self,jobAttempt):
        p = Popen(self._create_cmd_str(jobAttempt).split(' '),
                  stdout=open(jobAttempt.STDOUT_filepath,'w'),
                stderr=open(jobAttempt.STDERR_filepath,'w'),
                preexec_fn=preexec_function())
        jobAttempt.drmaa_jobID = p.pid
        current_processes[p.pid] = p
        all_processes[p.pid] = p

    def _check_for_finished_job(self):
        for k,p in current_processes.items():
            if p.poll() is not None:
                del current_processes[k]
                ja = JobAttempt.objects.get(jobManager=self,drmaa_jobID=p.pid)
                successful = p.poll() == 0
                ja._hasFinished(successful,{'exit_code':p.returncode})
                return ja
        return None

    def get_jobAttempt_status(self,jobAttempt):
        """
        Queries the DRM for the status of the job
        """
        try:
            r = all_processes[jobAttempt.drmaa_jobID].returncode
            if r is None:
                return 'job is running'
            elif r == 0:
                return 'job finished normally'
            else:
                'job finished, but failed'
        except KeyError:
            return 'has not been queued'


    def terminate_jobAttempt(self,jobAttempt):
        "Terminates a jobAttempt"
        try:
            current_processes[jobAttempt.drmaa_jobID].kill()
        except KeyError:
            pass


