from cosmos import session
import os
from django.utils.datastructures import SortedDict
from cosmos.utils.helpers import enable_stderr,disable_stderr
from cosmos.config import settings

from jobattempt import JobAttempt
from jobmanager import JobManagerBase

class JobStatusError(Exception):
    pass

#######################
# Initialize DRMAA
#######################

os.environ['DRMAA_LIBRARY_PATH'] = settings['drmaa_library_path']
if settings['DRM'] == 'LSF':
    os.environ['LSF_DRMAA_CONF'] = os.path.join(settings['cosmos_library_path'],'lsf_drmaa.conf')

import drmaa

if session.settings['DRM'] != 'local':
    drmaa_enabled = False
    try:
        drmaa_session = drmaa.Session()
        drmaa_session.initialize()
        drmaa_enabled = True
    except Exception as e:
        print e
        print "ERROR! Could not enable drmaa.  Proceeding without drmaa enabled."


decode_drmaa_state = SortedDict([
    (drmaa.JobState.UNDETERMINED, 'process status cannot be determined'),
    (drmaa.JobState.QUEUED_ACTIVE, 'job is queued and active'),
    (drmaa.JobState.SYSTEM_ON_HOLD, 'job is queued and in system hold'),
    (drmaa.JobState.USER_ON_HOLD, 'job is queued and in user hold'),
    (drmaa.JobState.USER_SYSTEM_ON_HOLD, 'job is queued and in user and system hold'),
    (drmaa.JobState.RUNNING, 'job is running'),
    (drmaa.JobState.SYSTEM_SUSPENDED, 'job is system suspended'),
    (drmaa.JobState.USER_SUSPENDED, 'job is user suspended'),
    (drmaa.JobState.DONE, 'job finished normally'),
    (drmaa.JobState.FAILED, 'job finished, but failed'),
    ]) #this is a sorted dictionary

class JobManager(JobManagerBase):
    """
    Note there can only be one of these instantiated at a time
    """
    class Meta:
        app_label = 'Job'
        db_table = 'Job_jobmanager'

    def get_jobAttempt_status(self,jobAttempt):
        """
        Queries the DRM for the status of the job
        """
        raise NotImplementedError

    def terminate_jobAttempt(self,jobAttempt):
        "Terminates a jobAttempt"
        try:
            drmaa_session.control(str(jobAttempt.drmaa_jobID), drmaa.JobControlAction.TERMINATE)
            return True
        except drmaa.errors.InternalException:
            False


    def get_jobAttempt_status(self,jobAttempt):
        """
        Queries the DRM for the status of the job
        """
        try:
            s = decode_drmaa_state[drmaa_session.jobStatus(str(jobAttempt.drmaa_jobID))]
        except drmaa.InvalidJobException:
            if jobAttempt.queue_status == 'completed':
                if jobAttempt.successful:
                    s = decode_drmaa_state[drmaa.JobState.DONE]
                else:
                    s = decode_drmaa_state[drmaa.JobState.FAILED]
            else:
                s = 'not sure' #job doesnt exist in queue anymore but didn't succeed or fail
        return s



    def __drmaa_createJobTemplate(self,jobAttempt):
        """
        Creates a JobTemplate for jobAttempt

        Possible attrs are:
        ['args','blockEmail','deadlineTime','delete','email','errorPath','hardRunDurationLimit'
        'hardWallclockTimeLimit','inputPath','jobCategory','jobEnvironment','jobName','jobSubmissionState',
        'joinFiles','nativeSpecification','outputPath','remoteCommand','softRunDurationLimit','softWallclockTimeLimit',
        'startTime','transferFiles','workingDirectory','cpu_time']

        """
        cmd = self._create_cmd_str(jobAttempt)

        jt = drmaa_session.createJobTemplate()
        jt.workingDirectory = session.settings['tmp_dir']
        #jt.remoteCommand = self.command_script_path
        #jt.args = self.command_script_text.split(' ')[1:]
        jt.remoteCommand = cmd.split(' ')[0]
        jt.args = cmd.split(' ')[1:]
        jt.workingDirectory = os.getcwd()
        jt.jobName = 'ja-'+jobAttempt.jobName
        jt.outputPath = ':'+jobAttempt.STDOUT_filepath
        jt.errorPath = ':'+jobAttempt.STDERR_filepath
        jt.nativeSpecification = get_drmaa_ns(jobAttempt)

        return jt

    def _submit_job(self,jobAttempt):
        jobTemplate = self.__drmaa_createJobTemplate(jobAttempt)
        jobAttempt.drmaa_jobID = drmaa_session.runJob(jobTemplate)
        jobTemplate.delete() #prevents memory leak


    def _check_for_finished_job(self):
        """
        Waits for any job to finish, and returns that JobAttempt.  If there are no jobs left, returns None.
        All the enable/disable stderr stuff is because LSF drmaa prints really annoying messages that mean nothing.
        """
        try:
            disable_stderr() #python drmaa prints whacky messages sometimes.  if the script just quits without printing anything, something really bad happend while stderr is disabled
            extra_jobinfo = drmaa_session.wait(jobId=drmaa.Session.JOB_IDS_SESSION_ANY,timeout=drmaa.Session.TIMEOUT_NO_WAIT)
            enable_stderr()
        except drmaa.errors.InvalidJobException as e:
            # There are no jobs to wait on.
            # This should never happen since I check for num_queued_jobs in yield_all_queued_jobs
            enable_stderr()
            raise Exception('drmaa_session.wait threw invalid job exception.  there are no jobs left.  make sure jobs are queued before calling _check_for_finished_job.')
        except drmaa.errors.ExitTimeoutException:
            #jobs are queued, but none are done yet
            enable_stderr()
            return None
        except Exception as e:
            enable_stderr()
            print e
        finally:
            enable_stderr()

        jobAttempt = JobAttempt.objects.get(drmaa_jobID = extra_jobinfo.jobId)

        extra_jobinfo = extra_jobinfo._asdict()

        successful = extra_jobinfo is not None and extra_jobinfo['exitStatus'] == 0 and extra_jobinfo['wasAborted'] == False
        jobAttempt._hasFinished(successful, extra_jobinfo)
        return jobAttempt


def get_drmaa_ns(jobAttempt):
    """Returns the DRM specific resource usage flags for the drmaa_native_specification
    :param time_limit: (int) as datetime.time object.
    :param mem_req: (int) memory required in MB
    :param cpu_req: (int) number of cpus required
    :param queue: (str) name of queue to submit to
    """
    task = jobAttempt.task
    DRM = session.settings['DRM']
    cpu_req = task.cpu_requirement
    mem_req = task.memory_requirement
    time_req = task.time_requirement
    queue = task.workflow.default_queue
    parallel_environment_name='' #deprecated

    if DRM == 'LSF':
        s = '-R "rusage[mem={0}] span[hosts=1]" -n {1}'.format(mem_req,cpu_req)
        if time_req:
            s += ' -W 0:{0}'.format(time_req)
        if queue:
            s += ' -q {0}'.format(queue)
        return s
    elif DRM == 'GE':
        return '-l h_vmem={mem_req}M,num_proc={cpu_req}'.format(
            mem_req=mem_req*1.5,
            pe= parallel_environment_name,
            cpu_req=cpu_req)
        #return '-l h_vmem={0}M,slots={1}'.format(mem_req,cpu_req)
    else:
        raise Exception('DRM not supported')