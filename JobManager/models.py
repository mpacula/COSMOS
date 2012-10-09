from django.db import models
import os,re,json,time,sys
from picklefield.fields import PickledObjectField
from django.utils.datastructures import SortedDict
from django.core.validators import RegexValidator
from Cosmos.helpers import check_and_create_output_dir,spinning_cursor
import cosmos_session
from cosmos_session import drmaa
from django.utils import timezone

import django.dispatch
jobAttempt_done = django.dispatch.Signal(providing_args=["jobAttempt"])

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


#real_stdout = os.dup(1)
real_stderr = os.dup(2)
def disable_stderr():
    devnull = os.open('/dev/null', os.O_WRONLY)
    os.dup2(devnull,2)
def enable_stderr():
    os.dup2(real_stderr,2)
  
class JobStatusError(Exception):
    pass
       
class JobAttempt(models.Model):
    """
    An attempt at running a node.
    """
    queue_status_choices = (
        ('not_queued','JobAttempt has not been submitted to the JobAttempt Queue yet'),
        ('queued','JobAttempt is in the JobAttempt Queue and is waiting to run, is running, or has finished'),
        ('completed','JobAttempt has completed'), #this means job.finished() has been executed.  use drmaa_state to find out if job was successful or failed.
    )
    state_choices = zip(decode_drmaa_state.keys(),decode_drmaa_state.values()) #dict2tuple
    
    created_on = models.DateTimeField(default=timezone.now())
    finished_on = models.DateTimeField(null=True,default=None)
    
    jobManager = models.ForeignKey('JobManager',related_name='+')
    
    #job status and input fields
    queue_status = models.CharField(max_length=150, default="not_queued",choices = queue_status_choices)
    successful = models.BooleanField(default=False)
    command = models.TextField(max_length=1000,default='')
    command_script_path = models.TextField(max_length=1000)
    jobName = models.CharField(max_length=150,validators = [RegexValidator(regex='^[A-Z0-9_]*$')])
    drmaa_output_dir = models.CharField(max_length=1000)
   
    #drmaa related input fields
    drmaa_native_specification = models.CharField(max_length=400, default='')
   
    #drmaa related and job output fields
    #drmaa_state = models.CharField(max_length=150, null=True, choices = state_choices) #drmaa state
    drmaa_jobID = models.BigIntegerField(null=True) #drmaa drmaa_jobID, note: not database primary key
    
    
    avg_data_mem = models.IntegerField(null=True,help_text='')
    max_data_mem = models.IntegerField(null=True,help_text='')
    avg_fdsize_mem = models.IntegerField(null=True,help_text='')
    avg_lib_mem = models.IntegerField(null=True,help_text='')
    avg_locked_mem = models.IntegerField(null=True,help_text='')
    avg_num_threads = models.IntegerField(null=True,help_text='')
    avg_pte_mem = models.IntegerField(null=True,help_text='')
    avg_rss_mem = models.IntegerField(null=True,help_text='')
    avg_virtual_mem = models.IntegerField(null=True,help_text='')
    block_io_delays = models.IntegerField(null=True,help_text='')
    cpu_time = models.IntegerField(null=True,help_text='')
    exit_status = models.IntegerField(null=True,help_text='')
    major_page_faults = models.IntegerField(null=True,help_text='')
    max_fdsize_mem = models.IntegerField(null=True,help_text='')
    max_lib_mem = models.IntegerField(null=True,help_text='')
    max_locked_mem = models.IntegerField(null=True,help_text='')
    max_num_threads = models.IntegerField(null=True,help_text='')
    max_pte_mem = models.IntegerField(null=True,help_text='')
    max_rss_mem = models.IntegerField(null=True,help_text='')
    max_virtual_mem = models.IntegerField(null=True,help_text='')
    minor_page_faults = models.IntegerField(null=True,help_text='')
    names = models.CharField(max_length=255,null=True,help_text='')
    nonvoluntary_context_switches = models.IntegerField(null=True,help_text='')
    num_polls = models.IntegerField(null=True,help_text='')
    num_processes = models.IntegerField(null=True,help_text='')
    percent_cpu = models.IntegerField(null=True,help_text='')
    pids = models.CharField(max_length=255,null=True,help_text='')
    single_proc_max_peak_rss = models.IntegerField(null=True,help_text='')
    single_proc_max_peak_virtual_rss = models.IntegerField(null=True,help_text='')
    system_time = models.IntegerField(null=True,help_text='')
    user_time = models.IntegerField(null=True,help_text='')
    voluntary_context_switches = models.IntegerField(null=True,help_text='')
    wall_time = models.IntegerField(null=True,help_text='')
    SC_CLK_TCK = models.IntegerField(null=True,help_text='')
    
    profile_fields = ['major_page_faults', 'max_num_threads', 'nonvoluntary_context_switches', 'exit_status', 'single_proc_max_peak_virtual_rss', 'avg_pte_mem', 'max_rss_mem', 'avg_num_threads', 'max_pte_mem', 'names', 'avg_locked_mem', 'pids', 'percent_cpu', 'avg_data_mem', 'wall_time', 'max_virtual_mem', 'num_polls', 'user_time', 'max_lib_mem', 'max_data_mem', 'num_processes', 'system_time', 'avg_fdsize_mem', 'minor_page_faults', 'avg_virtual_mem', 'avg_rss_mem', 'avg_lib_mem', 'single_proc_max_peak_rss', 'cpu_time', 'voluntary_context_switches', 'block_io_delays', 'max_fdsize_mem', 'max_locked_mem', 'SC_CLK_TCK']
    
    drmaa_info = PickledObjectField(null=True) #drmaa_info object returned by python-drmaa will be slow to access
    jobTemplate = None 
    _jobTemplate_attrs = ['args','blockEmail','deadlineTime','delete','email','errorPath','hardRunDurationLimit','hardWallclockTimeLimit','inputPath','jobCategory','jobEnvironment','jobName','jobSubmissionState','joinFiles','nativeSpecification','outputPath','remoteCommand','softRunDurationLimit','softWallclockTimeLimit','startTime','transferFiles','workingDirectory','cpu_time']
        
    @property
    def node(self):
        "This job's node"
        return self.node_set.get()
            
    @property
    def resource_usage(self):
        ":returns: (name,val,help,type)"
        for field in self.profile_fields:
            yield field, getattr(self,field), self._meta.get_field(field).help_text, 'na'
                    
    def update_from_profile(self):
        """Updates the resource usage from profile output"""
        try:
            p = json.load(file(self.profile_output_path,'r'))
            for k,v in p.items():
                setattr(self,k,v)
        except ValueError:
            "Probably empty resource usage because command didn't exist"
            pass
        except IOError:
            "Job probably immediately failed so there's no job data"
            pass
        
    def createJobTemplate(self,base_template):
        """
        Creates a JobTemplate.
        ``base_template`` must be passed down by the JobManager
        """
        self.jobTemplate = base_template
        self.jobTemplate.workingDirectory = os.getcwd()
        self.jobTemplate.remoteCommand = self.command_script_path #TODO create a bash script that this this command_script_text
        #self.jobTemplate.args = self.command_script_text.split(' ')[1:]
        self.jobTemplate.jobName = 'ja-'+self.jobName
        self.jobTemplate.outputPath = ':'+os.path.join(self.drmaa_output_dir,'cosmos_id_{0}.stdout'.format(self.id))
        self.jobTemplate.errorPath = ':'+os.path.join(self.drmaa_output_dir,'cosmos_id_{0}.stderr'.format(self.id))
        self.jobTemplate.blockEmail = True
        self.jobTemplate.nativeSpecification = self.drmaa_native_specification
        #create dir if doesn't exist
        check_and_create_output_dir(self.drmaa_output_dir)
        
    def update_from_drmaa_info(self):
        """takes _drmaa_info objects and updates a JobAttempt's attributes like utime and exitStatus"""
        dInfo = self.drmaa_info
        if dInfo is None:
            self.successful = False
        else:
            self.successful = dInfo['exitStatus'] == 0 and dInfo['wasAborted'] == False
        self.save()
    
    def get_drmaa_STDOUT_filepath(self):
        """Returns the path to the STDOUT file"""
        files = os.listdir(self.drmaa_output_dir)
        try:
            #filename = filter(lambda x:re.search('(\.o{0}|{0}\.out)'.format(self.drmaa_jobID),x), files)[0]
            filename = filter(lambda x:re.search('(\d+.out)|stdout'.format(self.drmaa_jobID),x), files)[0]
            return os.path.join(self.drmaa_output_dir,filename)
        except IndexError:
            return None
    
    def get_drmaa_STDERR_filepath(self):
        """Returns the path to the STDERR file"""
        files = os.listdir(self.drmaa_output_dir)
        try:
            #filename = filter(lambda x:re.search('(\.e{0})|({0}\.err)'.format(self.drmaa_jobID),x), files)[0]
            filename = filter(lambda x:re.search('(\d.err)|stderr'.format(self.drmaa_jobID),x), files)[0]
            return os.path.join(self.drmaa_output_dir,filename)
        except IndexError:
            return None
        
    @property
    def get_drmaa_status(self):
        """
        Queries the DRM for the status of the job
        """
        try:
            s = decode_drmaa_state[cosmos_session.drmaa_session.jobStatus(str(self.drmaa_jobID))]
        except drmaa.InvalidJobException:
            if self.queue_status == 'completed':
                if self.successful:
                    s = decode_drmaa_state[drmaa.JobState.DONE]
                else:
                    s = decode_drmaa_state[drmaa.JobState.FAILED]
            else:
                s = 'not sure' #job doesnt exist in queue anymore but didn't succeed or fail
        return s
    
    @property
    def STDOUT_filepath(self):
        "Path to STDOUT"
        return self.get_drmaa_STDOUT_filepath()
    @property
    def STDERR_filepath(self):
        "Path to STDERR"
        return self.get_drmaa_STDERR_filepath()
    @property
    def STDOUT_txt(self):
        "Read the STDOUT file"
        path = self.STDOUT_filepath
        if path is None:
            return 'File does not exist.'
        else:
            with open(path,'rb') as f:
                return f.read()
    @property
    def STDERR_txt(self):
        "Read the STDERR file"
        path = self.STDERR_filepath
        if path is None:
            return 'File does not exist.'
        else:
            with open(path,'rb') as f:
                return f.read()
    @property
    def profile_output_path(self):
        "Read the profile.py output which contains verbose information on resource usage"
        return os.path.join(self.drmaa_output_dir,str(self.id)+'.profile')
        
    def get_command_shell_script_text(self):
        "Read the command.sh file"
        with open(self.command_script_path,'rb') as f:
            return f.read()
    
    def hasFinished(self,drmaa_info):
        """Function for JobManager to Run when this JobAttempt finishes"""
        self.queue_status = 'completed'
        if drmaa_info is not None:
            self.drmaa_info = drmaa_info._asdict()
        self.update_from_drmaa_info()
        self.update_from_profile()
        
        self.finished_on = timezone.now()
        self.save()
    
    @models.permalink    
    def url(self):
        return ('jobAttempt_view',[str(self.id)])
    
    def __str__(self):
        return 'JobAttempt [{0}] [drmaa_jobId:{1}]'.format(self.id,self.drmaa_jobID)
    
    def toString(self):
        attrs_to_list = ['command_script_text','successful','queue_status','STDOUT_filepath','STDERR_filepath']
        out = []
        for attr in attrs_to_list:
            out.append('{0}: {1}'.format(attr,getattr(self,attr)))
        
        return "\n".join(out)

class JobManager(models.Model):
    """
    Note there can only be one of these instantiated at a time
    """
    created_on = models.DateTimeField(default=timezone.now())
        
    @property
    def jobAttempts(self):
        "This JobManager's jobAttempts"
        return JobAttempt.objects.filter(jobManager=self)
        
    def __init__(self,*args,**kwargs):
        super(JobManager,self).__init__(*args,**kwargs)
    
            
#    def close_session(self):
#        #TODO delete all jobtemplates
#        cosmos_session.drmaa_session.exit()

#    def terminate_all_queued_or_running_jobAttempts(self):
#        for jobAttempt in JobAttempt.objects.filter(jobManager=self,queue_status='queued'):
#            self.terminate_jobAttempt(jobAttempt)
#        
    def terminate_jobAttempt(self,jobAttempt):
        "Terminates a jobAttempt"
        cosmos_session.drmaa_session.control(str(jobAttempt.drmaa_jobID), drmaa.JobControlAction.TERMINATE)

    def __create_command_sh(self,jobAttempt):
        """Create a sh script that will execute command"""
        with open(jobAttempt.command_script_path,'wb') as f:
            hp = cosmos_session.cosmos_settings.home_path
            f.write("#!/bin/sh\n")
            f.write("python {profile} -d {db} -f {profile_out} \\".format(profile = os.path.join(hp,'Cosmos/profile/profile.py'),
                                              db = jobAttempt.profile_output_path+'.sqlite',
                                              profile_out = jobAttempt.profile_output_path
                                              ))
            f.write("\n")
            f.write(jobAttempt.command)
        os.system('chmod 700 {0}'.format(jobAttempt.command_script_path))
        
    def add_jobAttempt(self, command, drmaa_output_dir, jobName = "Generic_Job_Name", drmaa_native_specification=''):
        """
        Adds a new JobAttempt
        :param command: The system command to run
        :param jobName: an optional name for the jobAttempt
        :param drmaa_output_dir: the directory to story the stdout and stderr files
        :param drmaa_native_specification: the drmaa_native_specifications tring
        """
        jobAttempt = JobAttempt(jobManager=self, command = command, jobName = jobName, drmaa_output_dir = drmaa_output_dir, drmaa_native_specification=drmaa_native_specification)
        cmd_script_file_path = os.path.join(jobAttempt.drmaa_output_dir,'command.sh')
        jobAttempt.command_script_path = cmd_script_file_path
        jobAttempt.save()
        jobAttempt.createJobTemplate(base_template = cosmos_session.drmaa_session.createJobTemplate())
        self.__create_command_sh(jobAttempt)
        return jobAttempt
        
        
    def get_jobs(self):
        """Returns a django query object with all jobs belonging to this JobManager"""
        return JobAttempt.objects.filter(jobManager=self)
            
    
    def submit_job(self,job):
        """Submits and runs a job"""
        if job.queue_status != 'not_queued':
            raise JobStatusError('JobAttempt has already been submitted')
        job.drmaa_jobID = cosmos_session.drmaa_session.runJob(job.jobTemplate)
        job.queue_status = 'queued'
        job.jobTemplate.delete() #prevents memory leak
        job.save()
        return job
        
#    def __waitForJob(self,job):
#        """
#        Waits for a job to finish
#        Returns a drmaa info object
#        """
#        if job.queue_status != 'queued':
#            raise JobStatusError('JobAttempt is not in the queue.  Make sure you submit() the job first, and make sure it hasn\'t alreay been collected.')
#        try:
#            drmaa_info = cosmos_session.drmaa_session.wait(job.drmaa_jobID, drmaa.Session.TIMEOUT_WAIT_FOREVER)
#        except Exception as e:
#            if e == "code 24: no usage information was returned for the completed job":
#                drmaa_info = None
#               
#        job.hasFinished(drmaa_info)
#        job.save()
#        return job
    
    def get_numJobsQueued(self):
        "The number of queued jobs."
        return self.get_jobs().filter(queue_status = 'queued').count()
    
    def _check_for_finished_job(self):
        """
        Waits for any job to finish, and returns that JobAttempt.  If there are no jobs left, returns None.
        """
        try:
            disable_stderr() #python drmaa prints whacky messages sometimes
            drmaa_info = cosmos_session.drmaa_session.wait(jobId=drmaa.Session.JOB_IDS_SESSION_ANY,timeout=drmaa.Session.TIMEOUT_NO_WAIT)
            enable_stderr()
        except drmaa.errors.InvalidJobException as e: #throws this when there are no jobs to wait on
            print e
            self.workflow.log.error('ddrmaa_session.wait threw invalid job exception.  there are no jobs left?')
            raise
        except Exception as e:
            if type(e) == drmaa.errors.ExitTimeoutException: #jobs are queued, but none are done yet
                return None
            #there was a real error
            self.workflow.log.error(e)
            return None
            
        job = JobAttempt.objects.get(drmaa_jobID = drmaa_info.jobId)
        job.hasFinished(drmaa_info)
        return job
    
    def yield_all_queued_jobs(self):
        "Yield all queued jobs."
        i=0
        while self.get_numJobsQueued() > 0:
            i+=1
            sys.stderr.write(spinning_cursor(i))
            try:
                j = self._check_for_finished_job()
                sys.stderr.write('\b')
            except drmaa.errors.InvalidJobException:
                break
            if j != None:
                yield j
            time.sleep(1)
            
    def delete(self,*args,**kwargs):
        "Deletes this job manager"
        self.jobAttempts.delete()
        super(JobManager,self).__init__(self,*args,**kwargs)
        
    def toString(self):
        return "JobAttempt Manager, created on %s" % self.created_on
        