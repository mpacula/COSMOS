from django.db import models, transaction
from django.db.models import Q
from cosmos.JobManager.models import JobAttempt,JobManager
import os,sys,re,signal
from cosmos.Cosmos.helpers import get_drmaa_ns,validate_name,validate_not_null, check_and_create_output_dir, folder_size, get_workflow_logger
from cosmos.Cosmos import helpers
from django.core.exceptions import ValidationError
from picklefield.fields import PickledObjectField, dbsafe_decode
from cosmos import session
from django.utils import timezone
import networkx as nx
import pygraphviz as pgv
from cosmos.contrib.step import _unnest
import hashlib

status_choices=(
                ('successful','Successful'),
                ('no_attempt','No Attempt'),
                ('in_progress','In Progress'),
                ('failed','Failed')
                )


class TaskError(Exception): pass
class WorkflowError(Exception): pass


class TaskFile(models.Model):
    """
    Task File
    """
    name = models.CharField(max_length=50,null=False)
    fmt = models.CharField(max_length=10,null=True) #file format
    path = models.CharField(max_length=250,null=True)
    

    def __init__(self,*args,**kwargs):
        fmt = kwargs.get('fmt',None)
        path = kwargs.get('path',None)
        name = kwargs.get('name',None)
        if not fmt and path:
            kwargs['fmt'] = re.search('\.(.+?)$',path).group(1)
        if not name and fmt:
            kwargs['name'] = fmt
        return super(TaskFile,self).__init__(*args,**kwargs)
        
    @property
    def sha1sum(self):
        return hashlib.sha1(file(self.path).read())
    
    def __str__(self):
        return "#F[{0}:{1}:{2}]".format(self.id,self.name,self.path)
    
class Workflow(models.Model):
    """   
    This is the master object.  It contains a list of :class:`Stage` which represent a pool of jobs that have no dependencies on each other
    and can be executed at the same time. 
    """
    name = models.CharField(max_length=250,unique=True)
    output_dir = models.CharField(max_length=250)
    jobManager = models.OneToOneField(JobManager,null=True, related_name='workflow')
    dry_run = models.BooleanField(default=False,help_text="don't execute anything")
    max_reattempts = models.SmallIntegerField(default=3)
    default_queue = models.CharField(max_length=255,default=None,null=True)
    
    created_on = models.DateTimeField(null=True,default=None)
    finished_on = models.DateTimeField(null=True,default=None)
    
    
    def __init__(self, *args, **kwargs):
        kwargs['created_on'] = timezone.now()
        super(Workflow,self).__init__(*args, **kwargs)
        
        validate_name(self.name)
        #Validate unique name
        if Workflow.objects.filter(name=self.name).exclude(pk=self.id).count() >0:
            raise ValidationError('Workflow with name {0} already exists.  Please choose a different one or use .__resume()'.format(self.name))
            
        check_and_create_output_dir(self.output_dir)
        
        self.log, self.log_path = get_workflow_logger(self) 
        
    @property
    def tasks(self):
        """Tasks in this Workflow"""
        return Task.objects.filter(stage__in=self.stage_set.all())
    
    @property
    def task_edges(self):
        """Edges in this Workflow"""
        return TaskEdge.objects.filter(parent__in=self.tasks)
    
    @property
    def task_tags(self):
        """TaskTags in this Workflow"""
        return TaskTag.objects.filter(task__in=self.tasks)
    
    @property
    def task_files(self):
        "TaskFiles in this Stage"
        return TaskFile.objects.filter(task_output_set__in=self.tasks)
    
    @property
    def wall_time(self):
        """Time between this workflowh's creation and finished datetimes.  Note, this is a timedelta instance, not seconds"""
        return self.finished_on - self.created_on if self.finished_on else timezone.now().replace(microsecond=0) - self.created_on
    
    @property
    def total_stage_wall_time(self):
        """
        Sum(stage_wall_times).  Can be different from workflow.wall_time due to workflow stops and resumes.
        """
        times = map(lambda x: x['finished_on']-x['started_on'],Stage.objects.filter(workflow=self).values('finished_on','started_on'))
        return reduce(lambda x,y: x+y, filter(lambda wt: wt,times))
    
    @property
    def stages(self):
        """Stages in this Workflow"""
        return self.stage_set.all()
    
    @property
    def file_size(self):
        """Size of the output directory"""
        return folder_size(self.output_dir)
        
    @property
    def log_txt(self):
        """Path to the logfile"""
        return file(self.log_path,'rb').read()
    
    @staticmethod
    def start(name=None, restart=False, dry_run=False, root_output_dir=None, default_queue=None):
        """
        Starts a workflow.  If a workflow with this name already exists, return the workflow.
        
        :param name: (str) A unique name for this workflow. All spaces are converted to underscores. Required.
        :param restart: (bool) Restart the workflow by deleting it and creating a new one. Optional.
        :param dry_run: (bool) Don't actually execute jobs. Optional.
        :param root_output_dir: (bool) Replaces the directory used in settings as the workflow output directory. If None, will use default_root_output_dir in the config file. Optional.
        :param default_queue: (str) Name of the default queue to submit jobs to. Optional.
        """
        
        if name is None:
            raise ValidationError('Name of a workflow cannot be None')
        name = re.sub("\s","_",name)
        
        if root_output_dir is None:
            root_output_dir = session.settings.default_root_output_dir
            
        if restart:
            wf = Workflow.__restart(name=name, root_output_dir=root_output_dir, dry_run=dry_run, default_queue=default_queue)
        elif Workflow.objects.filter(name=name).count() > 0:
            wf = Workflow.__resume(name=name, dry_run=dry_run, default_queue=default_queue)
        else:
            wf = Workflow.__create(name=name, dry_run=dry_run, root_output_dir=root_output_dir, default_queue=default_queue)
        
        #remove stale objects
        wf._delete_stale_objects()
        
        #terminate on ctrl+c
        def ctrl_c(signal,frame):
                wf.terminate()
        try:
            signal.signal(signal.SIGINT, ctrl_c)
        except ValueError: #signal only works in main thread and django complains
            pass
        
        return wf
        
    
    @staticmethod
    def __resume(name=None,dry_run=False, default_queue=None):
        """
        Resumes a workflow from the last failed task.
        
        :param name: (str) A unique name for this workflow
        :param dry_run: (bool) Don't actually execute jobs. Optional.
        :param root_output_dir: (bool) Replaces the directory used in settings as the workflow output directory. If None, will use default_root_output_dir in the config file. Optional.
        :param default_queue: (str) Name of the default queue to submit jobs to. Optional.
        """

        if Workflow.objects.filter(name=name).count() == 0:
            raise ValidationError('Workflow {0} does not exist, cannot resume it'.format(name))
        wf = Workflow.objects.get(name=name)
        wf.dry_run=dry_run
        wf.finished_on = None
        wf.default_queue=default_queue
        
        wf.save()
        wf.log.info('Resuming workflow.')
        Stage.objects.filter(workflow=wf).update(order_in_workflow=None)
        
        return wf

    @staticmethod
    def __restart(name=None,root_output_dir=None,dry_run=False,default_queue=None,prompt_confirm=True):
        """
        Restarts a workflow.  Will delete the old workflow and all of its files
        but will retain the old workflow id for convenience
        
        :param name: (name) A unique name for this workflow. All spaces are converted to underscores. 
        :param dry_run: (bool) Don't actually execute jobs. Optional.
        :param root_output_dir: (bool) Replaces the directory used in settings as the workflow output directory. If None, will use default_root_output_dir in the config file. Optional.
        :param default_queue: (str) Name of the default queue to submit jobs to. Optional.
        :param prompt_confirm: (bool) If True, will prompt the user for a confirmation before deleting the workflow.
        """
        wf_id = None
        if Workflow.objects.filter(name=name).count():
            if prompt_confirm:
                if not helpers.confirm("Are you sure you want to restart Workflow '{0}'?  All files will be deleted.".format(name),default=True,timeout=30):
                    print "Exiting."
                    sys.exit(1)
            old_wf = Workflow.objects.get(name=name)
            wf_id = old_wf.id
            old_wf.delete()
        
        new_wf = Workflow.__create(_wf_id=wf_id, name=name, root_output_dir=root_output_dir, dry_run=dry_run, default_queue=default_queue)
        
        return new_wf
                
    @staticmethod
    def __create(name=None,dry_run=False,root_output_dir=None,_wf_id=None,default_queue=None):
        """
        Creates a new workflow
        
        :param name: (str) A unique name for this workflow
        :param dry_run: (bool) Don't actually execute jobs. Optional.
        :param root_output_dir: (bool) Replaces the directory used in settings as the workflow output directory. If None, will use default_root_output_dir in the config file. Optional.
        :param default_queue: (str) Name of the default queue to submit jobs to. Optional.
        """
        if Workflow.objects.filter(id=_wf_id).count(): raise ValidationError('Workflow with this _wf_id already exists')
        check_and_create_output_dir(root_output_dir)
        output_dir = os.path.join(root_output_dir,name)
        
        wf = Workflow.objects.create(id=_wf_id,name=name, jobManager = JobManager.objects.create(),output_dir=output_dir, dry_run=dry_run, default_queue=default_queue)
        wf.log.info('Created Workflow {0}.'.format(wf))
        return wf
            
        
    def add_stage(self, name):
        """
        Adds a stage to this workflow.  If a stage with this name (in this Workflow) already exists,
        and it hasn't been added in this session yet, return the existing one.
        
        :parameter name: (str) The name of the stage, must be unique within this Workflow. Required.
        """
        #TODO name can't be "log" or change log dir to .log
        name = re.sub("\s","_",name)
        #self.log.info("Adding stage {0}.".format(name))
        #determine order in workflow
        m = Stage.objects.filter(workflow=self).aggregate(models.Max('order_in_workflow'))['order_in_workflow__max']
        if m is None:
            order_in_workflow = 1
        else:
            order_in_workflow = m+1
        
        stage_exists = Stage.objects.filter(workflow=self,name=name).count()>0
        _old_id = None
        if stage_exists:
            old_stage = Stage.objects.get(workflow=self,name=name)
            _old_id = old_stage.id
            if old_stage.status == 'failed' or old_stage.status == 'in_progress':
                unadded_stages = list(self.stages.filter(order_in_workflow=None)) + [ old_stage ] #TODO filter using DAG, so that only dependent stages are deleted
                unadded_stages_str = ', '.join(map(lambda x: x.__str__(), unadded_stages))
                if helpers.confirm('{0} has a status of failed.  Would you like to restart the workflow from here?  Answering yes will delete the following stages: {1}. Answering no will only delete and re-run unsuccessful jobs.'.format(old_stage,unadded_stages_str),default=True):
                    map(lambda b: b.delete(),unadded_stages)
                
        b, created = Stage.objects.get_or_create(workflow=self,name=name,id=_old_id)
        if created:
            self.log.info('Creating {0} from scratch.'.format(b))
        else:
            self.log.info('{0} already exists, loading it from history...'.format(b))
            self.finished_on = None #resuming, so reset this
            
        b.order_in_workflow = order_in_workflow
        b.save()
        return b

    def _delete_stale_objects(self):
        """
        Deletes objects that are stale from the database.  This should only happens when the program exists ungracefully.
        """
        #TODO implement a catch all exception so that this never happens.  i think i can only do this if scripts are not run directly
        for ja in JobAttempt.objects.filter(task_set=None): ja.delete()
    
    def terminate(self):
        """
        Terminates this workflow and Exits
        """
        self.log.warning("Terminating this workflow...")
        self.save()
        jobAttempts = self.jobManager.jobAttempts.filter(queue_status='queued')
        jids = [ ja.id for ja in jobAttempts ]
        drmaa_jids = [ ja.drmaa_jobID for ja in jobAttempts ]
        #jobIDs = ', '.join(ids)
        #cmd = 'qdel {0}'.format(jobIDs)
        for jid in drmaa_jids:
            cmd = 'qdel {0}'.format(jid)
            os.system(cmd)
        
        #this basically a bulk task._has_finished and jobattempt.hasFinished
        self.log.info("Marking all terminated JobAttempts as failed.")
        jobAttempts.update(queue_status='completed',finished_on = timezone.now())
        tasks = Task.objects.filter(_jobAttempts__in=jids)

        self.log.info("Marking all terminated Tasks as failed.")
        tasks.update(status = 'failed',finished_on = timezone.now())
        
        self.log.info("Marking all terminated Stages as failed.")
        stages = Stage.objects.filter(pk__in=tasks.values('stage').distinct())
        stages.update(status = 'failed',finished_on = timezone.now())
        
        self.finished()
        
        self.log.info("Exiting.")
        sys.exit(1)
    
    def get_all_tag_keys_used(self):
        """Returns a set of all the keyword tags used on any task in this workflow"""
        return set([ d['key'] for d in TaskTag.objects.filter(task__in=self.tasks).values('key') ])
    
    def save_resource_usage_as_csv(self,filename):
        """Save resource usage to filename"""
        import csv
        profile_fields = JobAttempt.profile_fields_as_list()
        keys = ['stage'] + list(self.get_all_tag_keys_used()) + profile_fields
        f = open(filename, 'wb')
        dict_writer = csv.DictWriter(f, keys)
        dict_writer.writer.writerow(keys)
        for stage_resources in self.yield_stage_resource_usage():
            dict_writer.writerows(stage_resources)

    def yield_stage_resource_usage(self):
        """
        :yields: A dict of all resource usage, tags, and the name of the stage of every task
        """
        for stage in self.stages:
            dicts = [ dict(nru) for nru in stage.yield_task_resource_usage() ]
            for d in dicts: d['stage'] = re.sub('_',' ',stage.name)
            yield dicts

    @transaction.commit_on_success
    def bulk_save_tasks(self,tasks):
        """
        Does a bulk insert of tasks.  Will delete unsuccessful tasks, and ignore already successful tasks
        
        :param tasks: (list) a list of tasks
        
        .. note:: this does not save task->taskfile relationships
        
        >>> tasks = [stage.new_task(name='1',pcmd='cmd1',save=False),stage.new_task(name='2',pcmd='cmd2',save=False,{},True)]
        >>> stage.bulk_add_tasks(tasks)
        """
        
        
#        #validation
#        if task_exists and task.successful:
#            if task.pre_command != pcmd:
#                self.log.error("You can't change the pcmd of a existing successful task (keeping the one from history). Use hard_reset=True if you really want to do this.")
#            if task.outputs != outputs:
#                self.log.error("You can't change the outputs of an existing successful task (keeping the one from history). Use hard_reset=True if you really want to do this.")
#        
#     
        # Delete unsuccessful tasks
        unsuccessful_tasks = list(self.tasks.filter(successful=False))
        for t in unsuccessful_tasks: #TODO bulk delete
            t.delete()
        self.log.info('Deleting {0} unsuccessful tasks'.format(len(unsuccessful_tasks)))
               
        #remove successful tasks       
        successful_task_tags = map(lambda t: dbsafe_decode(t),self.tasks.filter(successful=True).values('tags'))
        tasks = filter(lambda t: t.tags not in successful_task_tags,tasks) #TODO use sets to speed this up
        
        ### Bulk add tasks
        self.log.info("Bulk adding {0} tasks...".format(len(tasks)))
        
        #need to manually set IDs because there's no way to get them in the right order for tagging after a bulk create
        m = Task.objects.all().aggregate(models.Max('id'))['id__max']
        id_start =  m + 1 if m else 1
        for i,t in enumerate(tasks): t.id = id_start + i
        
        Task.objects.bulk_create(tasks)
        #create output directories
        for t in tasks:
            os.mkdir(t.output_dir)
            os.mkdir(t.job_output_dir) #this is not in JobManager because JobMaster should be not care about these details
        
        ### Bulk add tags
        #TODO validate that all tags use the same keywords
        tasktags = []
        for t in tasks:
            for k,v in t.tags.items():
                tasktags.append(TaskTag(task=t,key=k,value=v))
        self.log.info("Bulk adding {0} task tags...".format(len(tasktags)))
        TaskTag.objects.bulk_create(tasktags)
        
        return
    
    @transaction.commit_on_success
    def bulk_save_task_files(self,taskfiles):
        """
        :param taskfiles: [taskfile1,taskfile2,...] A list of taskfiles
        """
        ### Bulk add
        m = TaskFile.objects.all().aggregate(models.Max('id'))['id__max']
        id_start =  m + 1 if m else 1
        for i,t in enumerate(taskfiles): t.id = id_start + i
        TaskFile.objects.bulk_create(taskfiles)
        return
    
    @transaction.commit_on_success
    def bulk_save_task_edges(self,edges):
        """
        :param edges: [(parent, child),...] A list of tuples of parent -> child relationships
        """
        #Remove existing edges
        current_edges = self.task_edges.values('parent','child').values()
        edges = filter(lambda e: (e[0].id,e[1].id) not in current_edges,edges)
        
        ### Bulk add parents
        task_edges = map(lambda e: TaskEdge(parent=e[0],child=e[1]),edges)
        self.log.info("Bulk adding {0} task edges...".format(len(task_edges)))
        TaskEdge.objects.bulk_create(task_edges)
        
        return
    
#    @transaction.commit_on_success
#    def bulk_save_tasks(self,data):
#        """
#        Does a bulk insert to speedup adding lots of tasks.  Will filter out any None values in the tasks_and_tags list.
#        
#        :param data: [{'task':task,'tags':tags_dict,'task_exists':bool,'parents':parent_tasks},..] example values of dicts: [(task1,tags1,task_exists,parent_tasks),(task2,tags2,task_exists,parent_tasks),...]
#        
#        >>> tasks = [(stage.new_task(name='1',pcmd='cmd1',save=False),{},True,[]),(stage.new_task(name='2',pcmd='cmd2',save=False,{},True,[]))]
#        >>> stage.bulk_add_tasks(tasks)
#        """
#        
#        ### Bulk add tasks
#        self.log.info("Bulk adding {0} tasks...".format(len(data)))
#        filtered_data = filter(lambda x: not x['task_exists'],data)
#        if len(filtered_data) == 0:
#            return []
#        
#        #need to manually set IDs because there's no way to get them in the right order for tagging after a bulk create
#        m = Task.objects.all().aggregate(models.Max('id'))['id__max']
#        id_start =  m + 1 if m else 1
#        for i,d in enumerate(filtered_data): d['task'].id = id_start + i
#        
#        Task.objects.bulk_create(map(lambda d: d['task'], filtered_data))
#        #create output directories
#        for n in map(lambda d: d['task'], filtered_data):
#            os.mkdir(n.output_dir)
#            os.mkdir(n.job_output_dir) #this is not in JobManager because JobMaster should be not care about these details
#        
#        ### Bulk add tags
#        #TODO validate that all tags have the same keywords
#        tasktags = []
#        for d in filtered_data:
#            for k,v in d['tags'].items():
#                tasktags.append(TaskTag(task=d['task'],key=k,value=v))
#        self.log.info("Bulk adding {0} task tags...".format(len(tasktags)))
#        TaskTag.objects.bulk_create(tasktags)
#        
#        ### Bulk add parents
#        task_edges = []
#        for d in filtered_data:
#            for parent in d['parents']:
#                task_edges.append(TaskEdge(parent=parent,child=d['task'],tags=d['tags']))
#        self.log.info("Bulk adding {0} task edges...".format(len(task_edges)))
#        TaskEdge.objects.bulk_create(task_edges)
#        
#        return
            
    def delete(self, *args, **kwargs):
        """
        Deletes this workflow.
        """
        self.log.info("Deleting {0}...".format(self))
        
#        if kwargs.setdefault('delete_files',False):
#            kwargs.pop('delete_files')
#            self.log.info('Deleting directory {0}'.format(self.output_dir))
        if os.path.exists(self.output_dir):
            self.log.info('Deleting directory {0}...'.format(self.output_dir))
            os.system('rm -rf {0}'.format(self.output_dir))
            
        self.jobManager.delete()
        self.log.info('Bulk deleting JobAttempts...')
        JobAttempt.objects.filter(task_set__in = self.tasks).delete()
        self.log.info('Bulk deleting TaskTags...')
        self.task_tags.delete()
        self.log.info('Bulk deleting TaskEdges...')
        self.task_edges.delete()
        self.log.info('Bulk deleting Tasks...')
        self.tasks.delete()
        self.log.info('Bulk deleting TaskFiles...')
        self.task_files.delete()
        self.log.info('Deleting Stages...'.format(self.name))
        self.stages.delete()
        self.log.info('{0} Deleted.'.format(self))
        
        
        for h in list(self.log.handlers):
            h.close()
            self.log.removeHandler(h)
        
        super(Workflow, self).delete(*args, **kwargs)
                

    
    def _run_task(self,task):
        """
        Creates and submits and JobAttempt.
        
        :param task: the task to submit a JobAttempt for
        """
        #TODO fix this it's slow (do it in bulk when running a workflow?)
        if task.stage.status == 'no_attempt':
            task.stage.status = 'in_progress'
            task.stage.started_on = timezone.now()
            task.stage.save()
        task.status = 'in_progress'
        self.log.info('Running {0}'.format(task))
        
        task.exec_command = task.pre_command
        
        #set output_file paths to the task's job_output_dir
        for f in task.output_files:
            if not f.path:
                f.path = os.path.join(task.job_output_dir,'{0}.{1}'.format('out' if f.name == f.fmt else f.name,f.fmt))
                f.save()
                
        for m in re.findall('(#F\[(.+?):(.+?):(.+?)\])',task.exec_command):
            taskfile = TaskFile.objects.get(pk=m[1])
            task.exec_command = task.exec_command.replace(m[0],taskfile.path)
        
#        try:
#            task.exec_command = task.pre_command.format(output_dir=task.job_output_dir,outputs = task.outputs)
#        except KeyError:
#            helpers.formatError(task.pre_command,{'output_dir':task.job_output_dir,'outputs': task.outputs})
                
        #create command.sh that gets executed
        
        jobAttempt = self.jobManager.add_jobAttempt(command=task.exec_command,
                                     drmaa_output_dir=os.path.join(task.output_dir,'drmaa_out/'),
                                     jobName="",
                                     drmaa_native_specification=get_drmaa_ns(DRM=session.settings.DRM,
                                                                             mem_req=task.memory_requirement,
                                                                             cpu_req=task.cpu_requirement,
                                                                             queue=self.default_queue))
        
        task._jobAttempts.add(jobAttempt)
        if self.dry_run:
            self.log.info('Dry Run: skipping submission of job {0}.'.format(jobAttempt))
        else:
            self.jobManager.submit_job(jobAttempt)
            self.log.info('Submitted jobAttempt with drmaa jobid {0}.'.format(jobAttempt.drmaa_jobID))
        task.save()
        #self.jobManager.save()
        return jobAttempt

#    def run_stage(self,stage):
#        """
#        Runs any unsuccessful tasks of a stage
#        """
#        self.log.info('Running stage {0}.'.format(stage))
#        
#        if stage.successful:
#            self.log.info('{0} has already been executed successfully, skip run.'.format(stage))
#            return
#        for task in stage.tasks:
#            if task.successful:
#                self.log.info('{0} has already been executed successfully, skip run.'.format(task))
#            else:
#                self.log.debug('{0} has not been executed successfully yet.'.format(task))
#                self._run_task(task)


    def _reattempt_task(self,task,failed_jobAttempt):
        """
        Reattempt running a task.
        
        :param task: (Task) the task to reattempt
        :param failed_jobAttempt: (bool) the previously failed jobAttempt of the task
        :returns: (bool) True if another jobAttempt was submitted, False if the max jobAttempts has already been reached
        """
        numAttempts = task.jobAttempts.count()
        if not task.successful: #ReRun jobAttempt
            if numAttempts < self.max_reattempts:
                self.log.warning("JobAttempt {0} of task {1} failed, on attempt # {2}, so deleting failed output files and retrying".format(failed_jobAttempt, task,numAttempts))
                os.system('rm -rf {0}/*'.format(task.job_output_dir))
                self._run_task(task)
                return True
            else:
                self.log.warning("Task {0} has reached max_reattempts of {0}.  This task has failed".format(self, self.max_reattempts))
                self.status = 'failed'
                self.save()
                return False
    
            
#    def wait(self,stage=None,terminate_on_fail=False):
#        """
#        Waits for all executing tasks to finish.  Returns an array of the tasks that finished.
#        if stage is omitted or set to None, all running tasks will be waited on.
#        
#        :param stage: (Stage) wait for all of a stage's tasks to finish
#        :param terminate_on_fail: (bool) If True, the workflow will self terminate of any of the tasks of this stage fail `max_job_attempts` times
#        """
#        tasks = []
#        if stage is None:
#            self.log.info('Waiting on all tasks...')
#        else:
#            self.log.info('Waiting on stage {0}...'.format(stage))
#        
#        for jobAttempt in self.jobManager.yield_all_queued_jobs():
#            task = jobAttempt.task
#            #self.log.info('Finished {0} for {1} of {2}'.format(jobAttempt,task,task.stage))
#            tasks.append(task)
#            if jobAttempt.successful:
#                task._has_finished(jobAttempt)
#            else:
#                submitted_another_job = self._reattempt_task(task,jobAttempt)
#                if not submitted_another_job:
#                    task._has_finished(jobAttempt) #job has failed and out of reattempts
#                    if terminate_on_fail:
#                        self.log.warning("{0} has reached max_reattempts and terminate_on_fail==True so terminating.".format(task))
#                        self.terminate()
#            if stage and stage.is_done():
#                break;
#            
#                    
#        if stage is None: #no waiting on a stage
#            self.log.info('All tasks for this wait have completed!')
#        else:
#            self.log.info('All tasks for the wait on {0} completed!'.format(stage))
#        
#        return tasks 


    def run(self,terminate_on_fail=False):
        """
        Runs a workflow using the DAG of jobs
        
        :param terminate_on_fail: (bool) If True, the workflow will self terminate of any of the tasks of this stage fail `max_job_attempts` times
        """
        self.log.info("Generating DAG...")
        wfDAG = WorkflowManager(self)
        self.log.info("Running DAG.")
        
        ready_tasks = wfDAG.get_ready_tasks()
        for n in ready_tasks:
            wfDAG.queued_task(n)
            self._run_task(n)
        
        finished_tasks = []
        for jobAttempt in self.jobManager.yield_all_queued_jobs():
            task = jobAttempt.task
            #self.log.info('Finished {0} for {1} of {2}'.format(jobAttempt,task,task.stage))
            finished_tasks.append(task)
            if jobAttempt.successful:
                task._has_finished(jobAttempt)
                wfDAG.completed_task(task)
                for n in wfDAG.get_ready_tasks():
                    wfDAG.queued_task(n)
                    self._run_task(n)
            else:
                submitted_another_job = self._reattempt_task(task,jobAttempt)
                if not submitted_another_job:
                    task._has_finished(jobAttempt) #job has failed and out of reattempts
                    if terminate_on_fail:
                        self.log.warning("{0} has reached max_reattempts and terminate_on_fail==True so terminating.".format(task))
                        self.terminate()
                    
        self.finished()
        return finished_tasks
    
    def finished(self,delete_unused_stages=False):
        """
        Call at the end of every workflow.
        If there any left over jobs that have not been collected,
        It will wait for all of them them
        
        :param delete_unused_stages: (bool) Any stages and their output_dir from previous workflows that weren't loaded since the last create, __resume, or __restart, using add_stage() are deleted.
        
        """
        self._check_and_wait_for_leftover_tasks()
        
        self.log.debug("Cleaning up workflow")
        if delete_unused_stages:
            self.log.info("Deleting unused stages")
            for b in Stage.objects.filter(workflow=self,order_in_workflow=None): b.delete() #these stages weren't used again after a __restart
            
        self.finished_on = timezone.now()
        self.save()
        self.log.info('Finished.')
        
    def _check_and_wait_for_leftover_tasks(self):
        """Checks and waits for any leftover tasks"""
        if self.tasks.filter(status='in_progress').count()>0:
            self.log.warning("There are left over tasks in the queue, waiting for them to finish")
            self.wait()
               
    
    def restart_from_here(self):
        """
        Deletes any stages in the history that haven't been added yet
        """
        if helpers.confirm("Are you sure you want to run restart_from_here() on workflow {0} (All files will be deleted)? Answering no will simply exit.".format(self),default=True,timeout=30):
            self.log.info('Restarting Workflow from here.')
            for b in Stage.objects.filter(workflow=self,order_in_workflow=None): b.delete()
        else:
            sys.exit(1)
    
    def get_tasks_by(self,stage=None,tags={},op="and"):
        """
        Returns the list of tasks that are tagged by the keys and vals in tags dictionary
        
        :param op: (str) either 'and' or 'or' as the logic to filter tags with
        :param tags: (dict) tags to filter for
        :returns: (queryset) a queryset of the filtered tasks
        
        >>> task.get_tasks_by(op='or',tags={'color':'grey','color':'orange'})
        >>> task.get_tasks_by(op='and',tags={'color':'grey','shape':'square'})
        """
        
        if op == 'or':
            raise NotImplemented('sorry')
        
        if stage:
            tasks = stage.tasks
        else:
            tasks = self.tasks
            
        if tags == {}:
            return tasks    
        else:    
            for k,v in tags.items():
                tasks = tasks.filter(tasktag__key=k, tasktag__value=v)
                
            return tasks

    def get_task_by(self,tags={},stage=None,op="and"):
        """
        Returns the list of tasks that are tagged by the keys and vals in tags dictionary.
        
        :raises Exception: if more or less than one task is returned
        
        :param op: (str) Choose either 'and' or 'or' as the logic to filter tags with
        :param tags: (dict) A dictionary of tags you'd like to filter for
        :returns: (queryset) a queryset of the filtered tasks
        
        >>> task.get_task_by(op='or',tags={'color':'grey','color':'orange'})
        >>> task.get_task_by(op='and',tags={'color':'grey','color':'orange'})
        """
    
        tasks = self.get_tasks_by(stage=stage,op=op,tags=tags) #there's just one group of tasks with this tag combination
        n = tasks.count()
        if n>1:
            raise Exception("More than one task with tags {0} in {1}".format(tags,stage))
        elif n == 0:
            raise Exception("No tasks with with tags {0}.".format(tags))
        return tasks[0]
    
    def __str__(self):
        return 'Workflow[{0}] {1}'.format(self.id,re.sub('_',' ',self.name))
    
    @models.permalink    
    def url(self):
        return ('workflow_view',[str(self.id)])

class WorkflowManager():
    def __init__(self,workflow):
        self.workflow = workflow
        self.dag = self.createDiGraph()
        self.dag_queue = self.dag.copy()
        self.dag_queue.remove_nodes_from(map(lambda x: x['id'],workflow.tasks.filter(successful=True).values('id')))
        self.queued_tasks = []
    
    def queued_task(self,task):
        self.queued_tasks.append(task.id)
    
    def completed_task(self,task):
        self.dag_queue.remove_node(task.id)
        
    def get_ready_tasks(self):
        degree_0_tasks= map(lambda x:x[0],filter(lambda x: x[1] == 0,self.dag_queue.in_degree().items()))
        #TODO change query to .filter to increase speed
        return map(lambda n_id: Task.objects.get(pk=n_id),filter(lambda x: x not in self.queued_tasks,degree_0_tasks)) 
    
    def createDiGraph(self):
        dag = nx.DiGraph()
        dag.add_edges_from([(ne['parent'],ne['child']) for ne in self.workflow.task_edges.values('parent','child')])
        for stage in self.workflow.stages:
            stage_name = stage.name
            for n in stage.tasks.values('id','tags','status'):
                dag.add_node(n['id'],tags=dbsafe_decode(n['tags']),status=n['status'],stage=stage_name)
        return dag
    
    def createAGraph(self,dag):
        dag = pgv.AGraph(strict=False,directed=True,fontname="Courier",fontsize=11)
        dag.node_attr['fontname']="Courier"
        dag.node_attr['fontsize']=8
        dag.add_edges_from(dag.edges())
        for stage,tasks in helpers.groupby(dag.nodes(data=True),lambda x:x[1]['stage']):
            sg = dag.add_subgraph(name="cluster_{0}".format(stage),label=stage,color='lightgrey')
            for n,attrs in tasks:
                def truncate_val(kv):
                    v = "{0}".format(kv[1])
                    v = v if len(v) <10 else v[1:8]+'..'
                    return "{0}: {1}".format(kv[0],v)
                label = " \\n".join(map(truncate_val,attrs['tags'].items()))
                status2color = { 'no_attempt':'black','in_progress':'gold1','successful': 'darkgreen','failed':'darkred'}
                sg.add_node(n,label=label,URL='/Workflow/Task/{0}/'.format(n),target="_blank",color=status2color[attrs['status']])
            
        return dag
    
#    def simple_path(self,head,vs):
#        """
#        task is the current task, vs is visited tasks
#        """
#        ss = self.dag.successors(head)
#        vs.append(head)
#        if len(ss) == 0: return [head] + self.simple_path_up(head,vs)
#        if len(ss) == 1: return [head] + self.simple_path(ss[0],vs) + self.simple_path_up(head,vs)
#        if len(ss) > 1: return [head] + self.simple_path(ss[0],vs) + self.simple_path(ss[1],vs) + self.simple_path_up(head,vs)
#    
#    def simple_path_up(self,tail,vs):
#        ps = self.dag.predecessors(tail)
#        ps = map(lambda n: (n,self.dag.task[n]),ps)
#        ps = filter(lambda p: p[0] not in vs,ps) # TODO speedup by marking visited tasks instead of keeping what can become a huge list
#        ps = [ ps.next()[0] for stage, ps in helpers.groupby(ps,lambda p: p[1]['stage']) ]
#        return ps
#        if len(ps) == 0: return [task]
#        if len(ps) == 1: return [task,ps[0]]
#        if len(ps) > 1: return [task,ps[0],ps[1]]
        #if len(ps) > 2: return [task,ps[0],ps[1],ps[2]]
#        if len(ps) == 1: return [task] + self.simple_path_up(ps)
#        if len(ps) > 1: return [task] + self.simple_path_up(ps[0]) + self.simple_path_up(ps[1])
    def simple_path(self,head,vtasks,vedges):
        """
        task is the current task, vs is visited tasks
        """
        ss = self.dag.successors(head)[0:3]
        vtasks.append(head)
        if len(ss) > 0:
            vedges.append((head,ss[0]))
            self.simple_path(ss[0],vtasks,vedges)
        if len(ss) > 1:
            vedges.append((head,ss[1]))
            self.simple_path(ss[1],vtasks,vedges)
        
        ps = self.dag.predecessors(head)
        ps = filter(lambda n: n not in vtasks,ps) #TODO might be slow
        if len(ps) > 0:
            vedges.append((ps[0],head))
#        if len(ps) > 0:
#            ps = map(lambda n: (n,self.dag.task[n]),ps) #append attr dict
#            for stage, ps_b in helpers.groupby(ps,lambda p: p[1]['stage']):
#                print stage
#                print list(ps_b)
#                print '*'*72
#                try:
#                    p = ps_b.next()
#                    vedges.append((p[0],head))
#                    vtasks.append(p[0])
#                except StopIteration:
#                    pass
            
        return vedges
    
    def get_simple_dag(self):
        root = None
        for task,degree in self.dag.in_degree_iter():
            if degree == 0:
                root = task
                break
        edges = self.simple_path(root,[],[])
        g= nx.DiGraph()
        tasks = set(_unnest(edges))
        g.add_nodes_from(map(lambda n: (n,self.dag.task[n]),tasks))
        g.add_edges_from(edges)
        return g
        #return self.dag.subgraph(nx.dfs_tree(self.dag,root).tasks())
    
    
    def as_img(self,format="svg"):      
        #g = self.createAGraph(self.dag)
        g = self.createAGraph(self.get_simple_dag())
        #g=nx.to_agraph(self.get_simple_dag())
        g.layout(prog="dot")
        return g.draw(format=format)
        
    def __str__(self): 
        #g = self.createAGraph(self.dag)
        g = self.createAGraph(self.get_simple_dag())
        #g=nx.to_agraph(self.get_simple_dag())
        return g.to_string()


class Stage(models.Model):
    """
    A group of jobs that can be run independently.  See `Embarassingly Parallel <http://en.wikipedia.org/wiki/Embarrassingly_parallel>`_ .
    
    .. note:: A Stage should not be directly instantiated, use :py:func:`Workflow.models.Workflow.add_stage` to create a new stage.
    """
    name = models.CharField(max_length=200)
    workflow = models.ForeignKey(Workflow)
    order_in_workflow = models.IntegerField(null=True)
    status = models.CharField(max_length=200,choices=status_choices,default='no_attempt') 
    successful = models.BooleanField(default=False)
    started_on = models.DateTimeField(null=True,default=None)
    created_on = models.DateTimeField(null=True,default=None)
    finished_on = models.DateTimeField(null=True,default=None)
    
    
    def __init__(self,*args,**kwargs):
        kwargs['created_on'] = timezone.now()
        super(Stage,self).__init__(*args,**kwargs)
        
        validate_not_null(self.workflow)
        check_and_create_output_dir(self.output_dir)
        
        validate_name(self.name,self.name)
        #validate unique name
        if Stage.objects.filter(workflow=self.workflow,name=self.name).exclude(id=self.id).count() > 0:
            raise ValidationError("Stage names must be unique within a given Workflow. The name {0} already exists.".format(self.name))

    @property
    def log(self):
        return self.workflow.log
    
    @property
    def percent_done(self):
        """
        Percent of tasks that have completed
        """
        done = Task.objects.filter(stage=self,successful=True).count()
        total = self.num_tasks
        status = self.status
        if total == 0 or done == 0:
            if status == 'in_progress' or status == 'failed':
                return 1
            return 0
        r = int(100 * float(done) / float(total))
        return r if r > 1 else 1

    def get_sjob_stat(self,field,statistic):
        """
        Aggregates a task successful job's field using a statistic
        :param field: (str) name of a tasks's field.  ex: wall_time or avg_rss_mem
        :param statistic: (str) choose from ['Avg','Sum','Max','Min','Count']
        
        >>> stage.get_stat('wall_time','Avg')
        120
        """
        if statistic not in ['Avg','Sum','Max','Min','Count']:
            raise ValidationError('Statistic {0} not supported'.format(statistic))
        aggr_fxn = getattr(models, statistic)
        aggr_field = '{0}__{1}'.format(field,statistic.lower())
        return JobAttempt.objects.filter(successful=True,task_set__in = Task.objects.filter(stage=self)).aggregate(aggr_fxn(field))[aggr_field]
    
    def get_task_stat(self,field,statistic):
        """
        Aggregates a task's field using a statistic
        :param field: (str) name of a tasks's field.  ex: cpu_req, mem_req
        :param statistic: (str) choose from ['Avg','Sum','Max','Min','Count']
        
        >>> stage.get_stat('cpu_requirement','Avg')
        120
        """
        if statistic not in ['Avg','Sum','Max','Min','Count']:
            raise ValidationError('Statistic {0} not supported'.format(statistic))
        aggr_fxn = getattr(models, statistic)
        aggr_field = '{0}__{1}'.format(field,statistic.lower())
        r = Task.objects.filter(stage=self).aggregate(aggr_fxn(field))[aggr_field]
        return int(r) if r else r
        

    @property
    def file_size(self):
        "Size of the stage's output_dir"
        return folder_size(self.output_dir)
    
    @property
    def wall_time(self):
        """Time between this stage's creation and finished datetimes.  Note, this is a timedelta instance, not seconds"""
        return self.finished_on - self.started_on if self.finished_on else timezone.now().replace(microsecond=0) - self.started_on
    
    @property
    def output_dir(self):
        "Absolute path to this stage's output_dir"
        return os.path.join(self.workflow.output_dir,self.name)
    
    @property
    def tasks(self):
        "Queryset of this stage's tasks"
        return Task.objects.filter(stage=self)
    
    @property
    def task_edges(self):
        "Edges in this Stage"
        return TaskEdge.objects.filter(parent__in=self.tasks)
    
    @property
    def task_tags(self):
        "TaskTags in this Stage"
        return TaskTag.objects.filter(task__in=self.tasks)
    
    @property
    def task_files(self):
        "TaskFiles in this Stage"
        return TaskFile.objects.filter(task_output_set__in=self.tasks)
    
    @property
    def num_tasks(self):
        "The number of tasks in this stage"
        return Task.objects.filter(stage=self).count()
    
    @property
    def num_tasks_successful(self):
        "Number of successful tasks in this stage"
        return Task.objects.filter(stage=self,successful=True).count()
    
    def get_all_tag_keys_used(self):
        """Returns a set of all the keyword tags used on any task in this stage"""
        try:
            return self.tasks.all()[0].tags.keys()
        except IndexError:
            return {}
        except AttributeError:
            return set(map(lambda x: x['key'],TaskTag.objects.filter(task__in=self.tasks).values('key').distinct()))
        
    def yield_task_resource_usage(self):
        """
        :yields: (list of tuples) tuples contain resource usage and tags of all tasks.  The first element is the name, the second is the value.
        """
        #TODO rework with time fields
        for task in self.tasks: 
            sja = task.get_successful_jobAttempt()
            if sja: 
                yield [jru for jru in sja.resource_usage_short] + task.tags.items() #add in tags to resource usage tuples

    def new_task(self, name, pcmd, hard_reset=False, input_files=[], output_files=[], tags = {}, mem_req=0, cpu_req=1, time_limit=None):
        """
        Adds a task to the stage. If the task with this name (in this stage) already exists and was successful, just return the existing one.
        If the existing task was unsuccessful, delete it and all of its output files, and return a new task.
        :param name: (str) The name of the task. Must be unique within this stage. All spaces are converted to underscores. Required.
        :param pcmd: (str) The preformatted command to execute. Usually includes the special keywords {output_dir} and {outputs[key]} which will be automatically parsed. Required.
        :param hard_reset: (bool) Deletes this task and all associated files and start it fresh. Optional.
        :param tags: (dict) A dictionary keys and values to tag the task with. These tags can later be used by methods such as :py:meth:`~Workflow.models.stage.group_tasks_by` and :py:meth:`~Workflow.models.stage.get_tasks_by` Optional.
        :param mem_req: (int) How much memory to reserve for this task in MB. Optional.
        :param cpu_req: (int) How many CPUs to reserve for this task. Optional.
        :param time_limit: (datetime.time) Not implemented.
        :returns: If save=True, an instance of a Task. If save=False, returns (task,tags) where task is a Task, tags is a dict, and task_exists is a bool.
        """
        
        if pcmd == '' or pcmd is None:
            raise TaskError('pre_command cannot be blank')
        
        #TODO validate that this task has the same tag keys as all other tasks
        
        task_kwargs = {
                       'stage':self,
                       'name':name,
                       'tags':tags,
                       'pre_command':pcmd,
                       'memory_requirement':mem_req,
                       'cpu_requirement':cpu_req,
                       'time_limit':time_limit
                       }
        
        #delete if hard_reset
        if hard_reset:
            task_exists = Task.objects.filter(stage=self,tags=tags).count() > 0
            if task_exists:
                task = Task.objects.get(stage=self,tags=tags)
            if not task_exists:
                raise ValidationError("Cannot hard_reset task with name {0} as it doesn't exist.".format(name))
            task.delete()
    
        #Just instantiate a task
        t= Task(**task_kwargs)
        t.input_files_list = input_files
        t.output_files_list = output_files
        return t
        
#    def new_task(self, name, pcmd, outputs={}, hard_reset=False, tags = {}, parents=[], save=True, skip_checks=False, mem_req=0, cpu_req=1, time_limit=None):
#        """
#        Adds a task to the stage. If the task with this name (in this stage) already exists and was successful, just return the existing one.
#        If the existing task was unsuccessful, delete it and all of its output files, and return a new task.
#        :param name: (str) The name of the task. Must be unique within this stage. All spaces are converted to underscores. Required.
#        :param pcmd: (str) The preformatted command to execute. Usually includes the special keywords {output_dir} and {outputs[key]} which will be automatically parsed. Required.
#        :param outputs: (dict) a dictionary of outputs and their names. Optional.
#        :param hard_reset: (bool) Deletes this task and all associated files and start it fresh. Optional.
#        :param tags: (dict) A dictionary keys and values to tag the task with. These tags can later be used by methods such as :py:meth:`~Workflow.models.stage.group_tasks_by` and :py:meth:`~Workflow.models.stage.get_tasks_by` Optional.
#        :param save: (bool) If False, will not save the task to the database. Meant to be used in concert with :method:`Workflow.bulk_save_tasks`
#        :param skip_checks: (bool) If True, will assume the task doesn't exist. If the task actually does exist, there will likely be a crash later on.
#        :param parents: (list) A list of parent tasks that this task is dependent on. This is optional and only used by the DAG functionality.
#        :param mem_req: (int) How much memory to reserve for this task in MB. Optional.
#        :param cpu_req: (int) How many CPUs to reserve for this task. Optional.
#        :param time_limit: (datetime.time) Not implemented.
#        :returns: If save=True, an instance of a Task. If save=False, returns (task,tags) where task is a Task, tags is a dict, and task_exists is a bool.
#        """
#                #validation
#                
#        # name = re.sub("\s","_",name) #user convenience
#        #
#        # if name == '' or name is None:
#        # raise ValidationError('name cannot be blank')
#        if skip_checks and save:
#            raise ValidationError('Cannot skip checks and save a task.')
#
#        if pcmd == '' or pcmd is None:
#            raise TaskError('pre_command cannot be blank')
#        
#        #TODO validate that this task has the same tag keys as all other tasks
#        
#        task_kwargs = {
#                       'stage':self,
#                       'name':name,
#                       'tags':tags,
#                       'pre_command':pcmd,
#                       'outputs':outputs,
#                       'memory_requirement':mem_req,
#                       'cpu_requirement':cpu_req,
#                       'time_limit':time_limit
#                       }
#        if skip_checks:
#            task_exists = False
#        else:
#            task_exists = Task.objects.filter(stage=self,tags=tags).count() > 0
#            if task_exists:
#                task = Task.objects.get(stage=self,tags=tags)
#        
#            #delete if hard_reset
#            if hard_reset:
#                if not task_exists:
#                    raise ValidationError("Cannot hard_reset task with name {0} as it doesn't exist.".format(name))
#                task.delete()
#            
#            if task_exists and not task.successful:
#                self.log.info("{0} was unsuccessful last run.".format(task))
#                task.delete()
#        
#            #validation
#            if task_exists and task.successful:
#                if task.pre_command != pcmd:
#                    self.log.error("You can't change the pcmd of a existing successful task (keeping the one from history). Use hard_reset=True if you really want to do this.")
#                if task.outputs != outputs:
#                    self.log.error("You can't change the outputs of an existing successful task (keeping the one from history). Use hard_reset=True if you really want to do this.")
#        
#        if not task_exists:
#            if save:
#                #Create and save a task
#                task = Task.create(**task_kwargs)
#                for k,v in tags.items():
#                    TaskTag.objects.create(task=task,key=k,value=v) #this is faster than a task.tag, because task.tag also writes to task.tags
#                for n in parents:
#                    TaskEdge.objects.create(parent=n,child=task)
#                self.log.info("Created {0} in {1}, and saved to the database.".format(task,self))
#                
#                stage = task.stage
#                if stage.is_done():
#                    stage.status = 'in_progress'
#                    stage.successful = False
#                    stage.save()
#            else:
#                #Just instantiate a task
#                task = Task(**task_kwargs)
#                
#        if save:
#            return task
#        else:
#            return {'task':task,'tags':tags,'parents':parents,'task_exists':task_exists}


    def is_done(self):
        """
        :returns: True if this stage is finished successfully or failed, else False
        """
        return self.status == 'successful' or self.status == 'failed'

    def _are_all_tasks_done(self):
        """
        :returns: True if all tasks have succeeded or failed in this stage, else False
        """
        return self.tasks.filter(Q(status = 'successful') | Q(status='failed')).count() == self.tasks.count()
        
    def _has_finished(self):
        """
        Executed when this stage has completed running.
        All it does is sets status as either failed or successful
        """
        num_tasks = Task.objects.filter(stage=self).count()
        num_tasks_successful = self.num_tasks_successful
        num_tasks_failed = Task.objects.filter(stage=self,status='failed').count()
        if num_tasks_successful == num_tasks:
            self.successful = True
            self.status = 'successful'
            self.log.info('Stage {0} successful!'.format(self))
        elif num_tasks_failed + num_tasks_successful == num_tasks:
            self.status='failed'
            self.log.warning('Stage {0} failed!'.format(self))
        else:
            #jobs are not done so this shouldn't happen
            raise Exception('Stage._has_finished() called, but not all tasks are completed.')
        
        self.finished_on = timezone.now()
        self.save()
    
    def get_tasks_by(self,tags={},op='and'):
        """
        An alias for :func:`Workflow.get_tasks_by` with stage=self
        
        :returns: a queryset of filtered tasks
        """
        return self.workflow.get_tasks_by(stage=self, tags=tags, op=op)
    
    def get_task_by(self,tags={},op='and'):
        """
        An alias for :func:`Workflow.get_task_by` with stage=self
        
        :returns: a queryset of filtered tasks
        """
        return self.workflow.get_task_by(stage=self, op=op, tags=tags)
                
    def group_tasks_by(self,keys=[]):
        """
        Yields tasks, grouped by tags in keys.  Groups will be every unique set of possible values of tags.
        For example, if you had tasks tagged by color, and shape, and you ran func:`stage.group_tasks_by`(['color','shape']),
        this function would yield the group of tasks that exist in the various combinations of `colors` and `shapes`.
        So for example one of the yields might be (({'color':'orange'n'shape':'circle'}), [ orange_circular_tasks ])
        
        :param keys: The keys of the tags you want to group by.
        :yields: (a dictionary of this group's unique tags, tasks in this group).
        
        .. note:: a missing tag is considered as None and thus placed into a 'None' group with other untagged tasks.  You should generally try to avoid this scenario and have all tasks tagged by the keywords you're grouping by.
        """
        if keys == []:
            yield {},self.tasks
        else:
            task_tag_values = TaskTag.objects.filter(task__in=self.tasks, key__in=keys).values() #get this stage's tags
            #filter out any tasks without all keys
            
            task_id2tags = {}
            for task_id, ntv in helpers.groupby(task_tag_values,lambda x: x['task_id']):
                task_tags = dict([ (n['key'],n['value']) for n in ntv ])
                task_id2tags[task_id] = task_tags
            
            for tags,task_id_and_tags_tuple in helpers.groupby(task_id2tags.items(),lambda x: x[1]):
                task_ids = [ x[0] for x in task_id_and_tags_tuple ]
                yield tags, Task.objects.filter(pk__in=task_ids)
    
    def delete(self, *args, **kwargs):
        """
        Bulk deletes this stage and all files associated with it.
        """
        self.log.info('Deleting Stage {0}.'.format(self.name))
        if os.path.exists(self.output_dir):
            self.log.info('Deleting directory {0}...'.format(self.output_dir))
            os.system('rm -rf {0}'.format(self.output_dir))
        self.log.info('Bulk deleting JobAttempts...')
        JobAttempt.objects.filter(task_set__in = self.tasks).delete()
        self.log.info('Bulk deleting TaskTags...')
        self.task_tags.delete()
        self.log.info('Bulk deleting TaskFiles...')
        self.task_files.delete()
        self.log.info('Bulk deleting TaskEdges...')
        self.task_edges.delete()
        self.log.info('Bulk deleting Tasks...')
        self.tasks.delete()
        super(Stage, self).delete(*args, **kwargs)
        self.log.info('{0} Deleted.'.format(self))
    
    @models.permalink    
    def url(self):
        "The URL of this stage"
        return ('stage_view',[str(self.id)])
        
    def __str__(self):
        return 'Stage[{0}] {1}'.format(self.id,re.sub('_',' ',self.name))
            

class TaskTag(models.Model):
    """
    A SQL row that duplicates the information of Task.tags that can be used for filtering, etc.
    """
    task = models.ForeignKey('Task')
    key = models.CharField(max_length=63)
    value = models.CharField(max_length=255)
    
    
    def __str__(self):
        return "TaskTag[self.id] {self.key}: {self.value} for Task[{task.id}]".format(self=self,task=self.task)

class TaskEdge(models.Model):
    parent = models.ForeignKey('Task',related_name='parent_edge_set')
    child = models.ForeignKey('Task',related_name='child_edge_set')
#    tags = PickledObjectField(null=True,default={})
    "The keys associated with the relationship.  ex, the group_by parameter of a many2one" 
    
    def __str__(self):
        return "{0.parent}->{0.child}".format(self)

class Task(models.Model):
    """
    The object that represents the command line that gets executed.
    """
    _jobAttempts = models.ManyToManyField(JobAttempt,related_name='task_set')
    pre_command = models.TextField(help_text='preformatted command.  almost always will contain the special string {output} which will later be replaced by the proper output path')
    exec_command = models.TextField(help_text='the actual command that was executed',null=True)
    name = models.CharField(max_length=255,null=True)
    memory_requirement = models.IntegerField(help_text="Memory to reserve for jobs in MB",default=0,null=True)
    cpu_requirement = models.SmallIntegerField(help_text="Number of CPUs to reserve for this job",default=1)
    time_limit = models.TimeField(help_text="Maximum time for a job to run",default=None,null=True)
    stage = models.ForeignKey(Stage,null=True)
    successful = models.BooleanField(null=False)
    status = models.CharField(max_length=100,choices = status_choices,default='no_attempt')
    
    _output_files = models.ManyToManyField(TaskFile,related_name='task_output_set',null=True) #dictionary of outputs
    @property
    def output_files(self): return self._output_files.all()
    
    _input_files = models.ManyToManyField(TaskFile,related_name='task_input_set',null=True)
    @property
    def input_files(self): return self._input_files.all()
    
    tags = PickledObjectField(null=False)
    created_on = models.DateTimeField(null=True,default=None)
    finished_on = models.DateTimeField(null=True,default=None)
    
    
    def __init__(self, *args, **kwargs):
        kwargs['created_on'] = timezone.now()
        return super(Task,self).__init__(*args, **kwargs)
    
    @staticmethod
    def create(*args,**kwargs):
        """
        Creates a task.
        """
        task = Task.objects.create(*args,**kwargs)
        
        if Task.objects.filter(stage=task.stage,tags=task.tags).count() > 1:
            task.delete()
            raise ValidationError("Tasks belonging to a stage with the same tags detected! tags: {0}".format(task.tags))
        
        check_and_create_output_dir(task.output_dir)
        check_and_create_output_dir(task.job_output_dir) #this is not in JobManager because JobMaster should be not care about these details
            
        #Create task tags    
        if type(task.tags) == dict:
            for key,value in task.tags.items():
                TaskTag.objects.create(task=task,key=key,value=value)
                
        return task
    
    @property
    def workflow(self):
        "This task's workflow"
        return self.stage.workflow

    @property
    def parents(self):
        "This task's parents"
        return map(lambda n: n.parent, TaskEdge.objects.filter(child=self).all())

    @property
    def task_edges(self):
        return TaskEdge.objects.filter(Q(parent=self)|Q(child=self))
    
    @property
    def task_tags(self):
        return TaskTag.objects.filter(task=self)

    @property
    def log(self):
        "This task's workflow's log"
        return self.workflow.log

    @property
    def file_size(self,human_readable=True):
        "Task filesize"
        return folder_size(self.output_dir,human_readable=human_readable)
    
    @property
    def output_dir(self):
        "Task output dir"
        return os.path.join(self.stage.output_dir,str(self.id))
    
    @property
    def job_output_dir(self):
        """Where the job output goes"""
        return os.path.join(self.output_dir,'out')
    
    @property
    def output_paths(self):
        "Dict of this task's outputs appended to this task's output_dir."
        r = {}
        for key,val in self.outputs.items():
            r[key] = os.path.join(self.job_output_dir,val)
        return r
    
    @property
    def jobAttempts(self):
        "Queryset of this task's jobAttempts."
        return self._jobAttempts.all().order_by('id')
    
    @property
    def wall_time(self):
        "Task's wall_time"
        return self.get_successful_jobAttempt().wall_time if self.successful else None
    
    def numAttempts(self):
        "This task's number of job attempts."
        return self._jobAttempts.count()
    
    def get_successful_jobAttempt(self):
        """
        Get this task's successful job attempt.
        
        :return: this task's successful job attempt.  If there were no successful job attempts, returns None
        """
        jobs = self._jobAttempts.filter(successful=True)
        if len(jobs) == 1:
            return jobs[0]
        elif len(jobs) > 1:
            raise Exception('more than 1 successful job, something went wrong!')
        else:
            return None # no successful jobs


    def _has_finished(self,jobAttempt):
        """
        Executed whenever this task finishes by the workflow.
        
        Sets self.status to 'successful' or 'failed' and self.finished_on to 'current_timezone'
        Will also run self.stage._has_finished() if all tasks in the stage are done.
        """
        if self._jobAttempts.filter(successful=True).count():
            self.status = 'successful'
            self.successful = True
            self.log.info("{0} Successful!".format(self,jobAttempt))        
        else:
            self.status = 'failed'
            self.log.info("{0} Failed!".format(self,jobAttempt))
            
        self.finished_on = timezone.now()
        self.save()
        
        if self.stage._are_all_tasks_done(): self.stage._has_finished()
        
    def tag(self,**kwargs):
        """
        Tag this task with key value pairs.  If the key already exists, its value will be overwritten.
        
        >>> task.tag(color="blue",shape="circle")
        """
        #TODO don't allow tags called things like 'status' or other task attributes
        for key,value in kwargs.items():
            value = str(value)
            tasktag, created = TaskTag.objects.get_or_create(task=self,key=key,defaults= {'value':value})
            if not created:
                tasktag.value = value
            tasktag.save()
            self.tags[key] = value
            
    @models.permalink    
    def url(self):
        "This task's url."
        return ('task_view',[str(self.id)])


    def delete(self, *args, **kwargs):
        """
        Deletes this task and all files associated with it
        """
        self.log.info('Deleting {0} and it\'s output directory {1}'.format(self,self.output_dir))
        #todo delete stuff in output_paths that may be extra files
        for ja in self._jobAttempts.all(): ja.delete()
        self.task_tags.delete()
        self.task_edges.delete
        self.output_files.delete()
        if os.path.exists(self.output_dir):
            os.system('rm -rf {0}'.format(self.output_dir))
        super(Task, self).delete(*args, **kwargs)
    
    def __str__(self):
        return 'Task[{0}] {1} {2}'.format(self.id,self.stage.name,self.tags)

