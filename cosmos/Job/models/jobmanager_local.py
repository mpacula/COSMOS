from cosmos import session
from jobattempt import JobAttempt
from jobmanager import JobManagerBase
from subprocess import Popen

class JobStatusError(Exception):
    pass

all_processes = {}
current_processes = {}

class JobManager(JobManagerBase):
    """
    Note there can only be one of these instantiated at a time
    """
    class Meta:
        app_label = 'Job'
        db_table = 'Job_jobmanager'

    def _submit_job(self,jobAttempt):
        p = Popen(self._create_cmd_str(jobAttempt).split(' '),stdout=open(jobAttempt.STDOUT_filepath,'w'),
                                stdin=open(jobAttempt.STDERR_filepath,'w'))
        jobAttempt.drmaa_jobID = p.pid
        current_processes[p.pid] = p
        all_processes[p.pid] = p

    def _check_for_finished_job(self):
        for k,p in current_processes.items():
            if p.poll() is not None:
                del current_processes[k]
                ja = JobAttempt.objects.get(drmaa_jobID=p.pid)
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
                return 'running'
            if r:
                return 'finished, exit code {0}'.format(r)
        except KeyError:
            return 'has not been queued'


    def terminate_jobAttempt(self,jobAttempt):
        "Terminates a jobAttempt"
        current_processes[jobAttempt.drmaa_jobID].kill()


