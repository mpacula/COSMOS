"""
Workflow models
"""
import os
import sys
import re
import signal
import shutil

from django.db import models, transaction
from django.db.models import Q, Sum
from django.db.utils import IntegrityError
from django.core.exceptions import ValidationError
from django.utils import timezone

from cosmos import session
from cosmos.models import JobAttempt, JobManager
from cosmos.utils.helpers import validate_name, check_and_create_output_dir, folder_size, get_workflow_logger
from cosmos.utils import helpers
from .WorkflowManager import WorkflowManager
from .TaskFile import TaskFile
from .TaskTag import TaskTag
from .Task import Task


settings = session.settings

opj = os.path.join


class Workflow(models.Model):
    """
    This is the master object.  It contains a list of :class:`Stage` which represent a pool of jobs that have no dependencies on each other
    and can be executed at the same time.
    """

    class Meta:
        app_label = 'cosmos'
        db_table = 'cosmos_workflow'

    name = models.CharField(max_length=250, unique=True)
    output_dir = models.CharField(max_length=250, null=True)
    #jobmanager = models.OneToOneField('cosmos.JobManager', null=True)
    dry_run = models.BooleanField(default=False, help_text="don't execute anything")
    max_reattempts = models.SmallIntegerField(default=3)
    default_queue = models.CharField(max_length=255, default=None, null=True)
    max_cores = models.IntegerField(default=0, help_text="Maximum cores to use at one time during the workflow.  "
                                                         "Based off of the sum of the running tasks' cpu_requirement")
    delete_intermediates = models.BooleanField(default=False, help_text="Delete intermediate files")
    last_cmd_executed = models.CharField(max_length=255, default=None, null=True)
    description = models.CharField(max_length=255, default=None, null=True)
    comments = models.TextField(null=True, default=None)
    stage_graph = models.TextField(null=True, default=None)

    created_on = models.DateTimeField(null=True, default=None)
    finished_on = models.DateTimeField(null=True, default=None)

    def __init__(self, *args, **kwargs):
        kwargs['created_on'] = timezone.now()
        super(Workflow, self).__init__(*args, **kwargs)

        # set default output_dir
        if self.output_dir is None:
            self.output_dir = opj(session.settings['default_root_output_dir'],
                                  '{1}'.format(self, self.name.replace(' ', '_')))

        validate_name(self.name)

        self.log, self.log_path = get_workflow_logger(self)
        self.jobManager = JobManager(workflow=self)

    @property
    def tasks(self):
        """Tasks in this Workflow"""
        return Task.objects.filter(stage__in=self.stage_set.all())

    @property
    def jobAttempts(self):
        return JobAttempt.objects.filter(task__stage__workflow=self)

    @property
    def task_edges(self):
        return Task._parents.through.objects.filter(to_task__stage__workflow=self)

    @property
    def tasktags(self):
        """TaskTags in this Workflow"""
        return self.tasktag_set()

    @property
    def task_files(self):
        "TaskFiles in this Stage"
        return TaskFile.objects.filter(task_output_set__in=self.tasks)

    @property
    def wall_time(self):
        """Time between this workflow's creation and finished datetimes.  Note, this is a timedelta instance, not seconds"""
        return self.finished_on - self.created_on if self.finished_on else timezone.now().replace(
            microsecond=0) - self.created_on

    # not sure if this works so commented
    # @property
    # def total_stage_wall_time(self):
    #     """
    #     Sum(stage_wall_times).  Can be different from workflow.wall_time due to workflow stops and reloads.
    #     """
    #     times = map(lambda x: x['finished_on']-x['started_on'],Stage.objects.filter(workflow=self).values('finished_on','started_on'))
    #     return reduce(lambda x,y: x+y, filter(lambda wt: wt,times))

    @property
    def stages(self):
        """Stages in this Workflow"""
        return self.stage_set.all()

    @property
    def file_size(self, human_readable=True):
        """Size of the output directory"""
        return folder_size(self.output_dir, human_readable=human_readable)

    @property
    def log_txt(self):
        """Path to the logfile"""
        return file(self.log_path, 'rb').read()

    @staticmethod
    def start(name, **kwargs):
        """
        Starts a workflow.  If a workflow with this name already exists, return the workflow.

        :param name: (str) A unique name for this workflow. All spaces are converted to underscores. Required.
        :param restart: (bool) Complete restart the workflow by deleting it and creating a new one. Optional.
        :param dry_run: (bool) Don't actually execute jobs. Optional.
        :param output_dir: (output_dir) The output directory for the workflow.
          If None, will use a subdir with the workflow's name in the default_root_output_dir in the config file. Optional.
        :param default_queue: (str) Name of the default queue to submit jobs to. Optional.
        :param delete_intermediates: (str) Deletes intermediate files to save scratch space.
        :param max_cores: (int) maximum number of cores
        """

        kwargs.setdefault('dry_run', False)
        kwargs.setdefault('default_queue', settings['default_queue'])
        kwargs.setdefault('delete_intermediates', False)
        kwargs.setdefault('comments', None)
        kwargs.setdefault('max_cores', 0)

        restart = kwargs.pop('restart', False)
        prompt_confirm = kwargs.pop('prompt_confirm', True)

        #name = re.sub("\s","_",name)

        if restart:
            wf = Workflow.__restart(name=name, prompt_confirm=prompt_confirm, **kwargs)
        elif Workflow.objects.filter(name=name).count() > 0:
            wf = Workflow.__reload(name=name, prompt_confirm=prompt_confirm, **kwargs)
        else:
            wf = Workflow.__create(name=name, **kwargs)

        #remove stale objects
        wf._delete_stale_objects()

        #terminate on ctrl+c
        def ctrl_c(signal, frame):
            wf.terminate()

        try:
            signal.signal(signal.SIGINT, ctrl_c)
        except ValueError: #signal only works in parse_args thread and django complains
            pass

        return wf

    @staticmethod
    def __resume(name, dry_run, default_queue, delete_intermediates, comments, max_cores, **kwargs):
        """
        Resumes a workflow without deleting any unsuccessful tasks.  Provides a way to override workflow
        properties.
        Probably won't be called by anything except __reload

        see :py:meth:`start` for parameter definitions
        """

        if Workflow.objects.filter(name=name).count() == 0:
            raise ValidationError('Workflow {0} does not exist, cannot resume it'.format(name))
        wf = Workflow.objects.get(name=name)
        wf.dry_run = dry_run
        wf.default_queue = default_queue
        wf.delete_intermediates = delete_intermediates
        wf.max_cores = max_cores
        if comments:
            wf.comments = comments

        wf.save()
        wf.log.info('Resuming {0}'.format(wf))
        return wf

    @staticmethod
    def __reload(name, dry_run, default_queue, delete_intermediates, prompt_confirm=True, **kwargs):
        """
        Resumes a workflow, keeping successful tasks and deleting unsuccessful ones.

        see :py:meth:`start` for parameter definitions
        """
        #TODO create a delete_stages(stages) method, that works faster than deleting individual stages
        #TODO ideally just change the queryset manager to do this automatically when a stages.delete() is called
        if prompt_confirm and not helpers.confirm(
                "Reloading the workflow, are you sure you want to delete any unsuccessful tasks in '{0}'?".format(name),
                default=True, timeout=120):
            print "Exiting."
            sys.exit(1)

        wf = Workflow.__resume(name, dry_run, default_queue, delete_intermediates, **kwargs)
        wf.finished_on = None

        #delete a stage if ALL tasks are unsuccessful
        for s in wf.stages.filter(workflow=wf).exclude(task__successful=True):
            wf.log.info('{0} has no successful tasks.'.format(s))
            s.delete()

        #Delete unsuccessful tasks
        utasks = wf.tasks.filter(successful=False)
        num_utasks = len(utasks)
        if num_utasks > 0:
            wf.bulk_delete_tasks(utasks)

        wf.save()
        return wf

    @staticmethod
    def __restart(name, prompt_confirm=True, **kwargs):
        """
        Restarts a workflow.  Will delete the old workflow and all of its files
        but will retain the old workflow id for convenience

        see :py:meth:`start` for parameter definitions

        """
        wf_id = None
        if Workflow.objects.filter(name=name).count():
            if prompt_confirm and not helpers.confirm(
                    "Are you sure you want to restart Workflow '{0}'?  All files will be deleted.".format(name),
                    default=True, timeout=120):
                print "Exiting."
                sys.exit(1)
            old_wf = Workflow.objects.get(name=name)
            wf_id = old_wf.id
            old_wf.delete()

        new_wf = Workflow.__create(_wf_id=wf_id, name=name, **kwargs)

        return new_wf

    @staticmethod
    def __create(name, _wf_id=None, **kwargs):
        """
        Creates a new workflow

        see :py:meth:`start` for parameter definitions
        :param _wf_id: the ID to use for creating a workflow
        """
        if Workflow.objects.filter(id=_wf_id).count():
            raise ValidationError('Workflow with this _wf_id already exists')
        output_dir = kwargs.setdefault('output_dir', None)
        if output_dir and os.path.exists(output_dir):
            raise ValidationError('output directory {0} already exists'.format(output_dir))

        wf = Workflow.objects.create(id=_wf_id, name=name, **kwargs)
        wf.save()
        check_and_create_output_dir(wf.output_dir)

        wf.log.info('Created Workflow {0}.'.format(wf))

        return wf


    def add_stage(self, name):
        """
        Adds a stage to this workflow.  If a stage with this name (in this Workflow) already exists,
        and it hasn't been added in this session yet, return the existing one after removing its
        finished_on datetimestamp and resetting it's order_in_workflow

        :param name: (str) The name of the stage, must be unique within this Workflow. Required.
        """
        #TODO name can't be "log" or change log dir to .log
        name = re.sub("\s", "_", name)

        stage, created = self.stages.get_or_create(workflow=self, name=name)
        min, max = self.stages.aggregate(
            models.Max('order_in_workflow'),
            models.Min('order_in_workflow')
        ).values()
        max = 0 if max is None else max
        if created:
            self.log.info('Creating {0}'.format(stage))
            stage.order_in_workflow = max + 1
        else:
            self.log.info('Loading {0}'.format(stage))
            self.finished_on = None

        stage.save()
        return stage

    def _delete_stale_objects(self):
        """
        Deletes objects that are stale from the database.  This should only happens when the program exists ungracefully.
        """
        #TODO implement a catch all exception so that this never happens.  i think i can only do this if scripts are not run directly
        JobAttempt.objects.filter(task=None).delete()
        TaskFile.objects.filter(task_output_set=None).delete()
        TaskTag.objects.filter(task=None).delete()


    def terminate(self, exit=True):
        """
        Terminates this workflow and Exits
        :param exception: an exception to raise after terminating
        """
        self.log.warning("Terminating {0}...".format(self))
        self.save()
        jobAttempts = self.jobManager.jobAttempts.filter(queue_status='queued')
        self.log.info("Sending Terminate signal to all running jobs.")
        for ja in jobAttempts:
            self.jobManager.terminate_jobAttempt(ja)

        #this basically a bulk task._has_finished and jobattempt.hasFinished
        task_ids = jobAttempts.values('task')
        tasks = Task.objects.filter(pk__in=task_ids)

        self.log.info("Marking {0} terminated Tasks as failed.".format(tasks.count()))
        tasks.update(status='failed', finished_on=timezone.now())

        stages = self.stages.filter(Q(task__in=tasks) | Q(successful=False))
        self.log.info("Marking {0} terminated Stages as failed.".format(stages.count()))
        stages.update(status='failed', finished_on=timezone.now())

        self.log.info("Marking {0} terminated JobAttempts as failed.".format(len(jobAttempts)))
        jobAttempts.update(queue_status='finished', finished_on=timezone.now())

        self.comments = "{0}<br/>{1}".format(self.comments if self.comments else '', "terimate()ed")

        self.finished()

        self.log.info("Exiting.")

        if not exit:
            return
        else:
            sys.exit(1)

    def get_all_tag_keys_used(self):
        """Returns a set of all the keyword tags used on any task in this workflow"""
        return set([d['key'] for d in TaskTag.objects.filter(task__in=self.tasks).values('key')])

    def save_resource_usage_as_csv(self, filename):
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
            dicts = [dict(nru) for nru in stage.yield_task_resource_usage()]
            for d in dicts: d['stage'] = re.sub('_', ' ', stage.name)
            yield dicts

    @transaction.commit_on_success
    def bulk_save_tasks(self, tasks):
        """
        Does a bulk insert of tasks.  Identical tasks should not be in the database.

        :param tasks: (list) a list of tasks

        .. note:: this does not save task->taskfile relationships

        >>> tasks = [stage.new_task(pcmd='cmd1',save=False,{'i':1}),stage.new_task(pcmd='cmd2',save=False,{'i':2})]
        >>> stage.bulk_save_tasks(tasks)
        """
        self.log.info("Bulk adding {0} Tasks...".format(len(tasks)))

        #need to manually set IDs because there's no way to get them in the right order for tagging after a bulk create
        m = Task.objects.all().aggregate(models.Max('id'))['id__max']
        id_start = m + 1 if m else 1
        for i, t in enumerate(tasks): t.id = id_start + i

        # try:
        Task.objects.bulk_create(tasks)
        # except IntegrityError as e:
        #     for tpl, tasks in helpers.groupby(tasks + list(self.tasks), lambda t: (t.tags,t.stage)):
        #         if len(list(tasks)) > 1:
        #             print 'ERROR! Duplicate tags in {0}, which are:'.format(tpl[1])
        #             pprint.pprint(tpl[0])
        #
        #     raise(IntegrityError('{0}'.format(e)))

        #create output directories
        for t in tasks:
            os.system('mkdir -p {0}'.format(t.job_output_dir))
            #os.mkdir(t.job_output_dir) #this is not in JobManager because JobManager should be not care about these details

        ### Bulk add tags
        tasktags = []
        for t in tasks:
            for k, v in t.tags.items():
                tasktags.append(TaskTag(task=t, key=k, value=v))
        self.log.info("Bulk adding {0} TaskTags...".format(len(tasktags)))
        TaskTag.objects.bulk_create(tasktags)

        ### Reset status of stages with new tasks
        #        reset_stages_pks = set(map(lambda t: t.stage.pk, tasks))
        #        Stage.objects.filter(id__in=reset_stages_pks).update(status="no_attempt",finished_on=None)

        return

    @transaction.commit_on_success
    def bulk_save_taskfiles(self, taskfiles):
        """
        :param taskfiles: (list) A list of taskfiles.
        """
        self.log.info("Bulk adding {0} TaskFiles...".format(len(taskfiles)))
        m = TaskFile.objects.all().aggregate(models.Max('id'))['id__max']
        id_start = m + 1 if m else 1
        for i, t in enumerate(taskfiles):
            t.id = id_start + i
        try:
            TaskFile.objects.bulk_create(taskfiles)
        except IntegrityError as e:
            return '{0}.  There are probably multiple tasks with the same output files'.format(e)

    @transaction.commit_on_success
    def bulk_delete_tasks(self, tasks):
        """
        Bulk deletes tasks and their related objects to this Workflow.  Does NOT delete empty stages

        :param tasks: either a list of tasks or a queryset of tasks
        """

        if isinstance(tasks, list):
            tasks = Task.objects.filter(pk__in=[t.id for t in tasks])

        task_output_dirs = map(lambda t: t.output_dir, tasks)

        self.log.info("Bulk deleting {0} tasks".format(len(tasks)))
        #self.log.info('Bulk deleting JobAttempts...')
        JobAttempt.objects.filter(task__in=tasks).delete()
        #self.log.info('Bulk deleting TaskTags...')
        TaskTag.objects.filter(task__in=tasks).delete()
        #self.log.info('Bulk deleting TaskFiles...')
        TaskFile.objects.filter(task_output_set__in=tasks).delete()
        #self.log.info('Bulk deleting Tasks...')
        tasks.delete()

        self.log.info('Deleting Task output directories')
        for d in task_output_dirs:
            os.system('rm -rf {0}'.format(d))

        # Update stages that are not longer successful
        self.stages.filter(successful=True, task__successful=False).update(
            successful=False, status='in_progress', finished_on=None
        )


    #TODO this probably doesn't have to be a transaction
    @transaction.commit_on_success
    def delete(self, *args, **kwargs):
        """
        Deletes this workflow.
        """
        self.log.info("Deleting {0} and it's output dir {1}...".format(self, self.output_dir))
        save_str_representation = str(self)
        wf_output_dir = self.output_dir

        #self.jobmanager.delete()
        self.bulk_delete_tasks(self.tasks)
        self.log.info('Bulk Deleting Stages...'.format(self.name))
        self.stages.delete()

        super(Workflow, self).delete(*args, **kwargs)

        self.log.info('{0} Deleted.'.format(save_str_representation))
        x = list(self.log.handlers)
        for h in x:
            self.log.removeHandler(h)
            h.flush()
            h.close()

        if os.path.exists(wf_output_dir):
            os.system('rm -rf {0}'.format(self.output_dir))

    def _run_task(self, task):
        """
        Creates and submits and JobAttempt.

        :param task: the task to submit a JobAttempt for
        """
        if (task.NOOP):
            return 'NOOP'

        #TODO fix this it's slow (do it in bulk when running a workflow?)
        # Make sure Stag attributes correctly reflect the current state
        if task.stage.status in ['no_attempt', 'failed']:
            if task.stage.status == 'no_attempt':
                task.stage.started_on = timezone.now()
            task.stage.set_status('in_progress')
            task.stage.save()
        task.set_status('in_progress')
        self.log.info('Running {0}'.format(task))

        task.exec_command = task.pcmd

        #set output_file paths to the task's job_output_dir
        for f in task.output_files:
            if not f.path:
                basename = '{0}.{1}'.format(task.stage.name if f.name == f.fmt else f.name,
                                            f.fmt) if not f.basename else f.basename
                #basename = task.tags_as_query_string().replace('&','__').replace('=','_')+'.'+f.fmt if not f.basename else f.basename

                f.path = os.path.join(task.job_output_dir, basename)
                f.save()
            if f.fmt == 'dir':
                check_and_create_output_dir(f.path)

        #Replace TaskFile hashes with their paths
        for m in re.findall('(#F\[(.+?):(.+?):(.+?)\])', task.exec_command):
            try:
                taskfile = TaskFile.objects.get(pk=m[1])
                task.exec_command = task.exec_command.replace(m[0], taskfile.path)
            except ValueError as e:
                raise ValueError('{0}.  Task is {1}. Taskfile str is {2}'.format(e, task, m[0]))
            except TypeError as e:
                raise TypeError("{0}. m[0] is {0} and taskfile is {1}".format(m[0], taskfile))

        jobAttempt = self.jobManager.add_jobAttempt(
            task=task,
            command=task.exec_command,
            jobName=""
        )

        #task.jobattempt_set.add(jobAttempt)
        if self.dry_run:
            self.log.info('Dry Run: skipping submission of job {0}.'.format(jobAttempt))
        else:
            self.jobManager.submit_job(jobAttempt)
            self.log.info('Submitted jobAttempt with drmaa jobid {0}.'.format(jobAttempt.drmaa_jobID))

        task.save()
        return jobAttempt


    def _reattempt_task(self, task, failed_jobAttempt):
        """
        Reattempt running a task.

        :param task: (Task) the task to reattempt
        :param failed_jobAttempt: (bool) the previously failed jobAttempt of the task
        :returns: (bool) True if another jobAttempt was submitted, False if the max jobAttempts has already been reached.
        """
        numAttempts = task.jobAttempts.count()
        if not task.successful: #ReRun jobAttempt
            self.log.warning(
                "{0} of {1} failed, on attempt # {2}, so deleting failed output files and retrying.\n".format(
                    failed_jobAttempt, task, numAttempts)
                + "<COMMAND path=\"{1}\">\n{0}\n</COMMAND>\n".format(failed_jobAttempt.get_command_shell_script_text(),
                                                                     failed_jobAttempt.command_script_path)
                + "<STDERR>\n{0}\n</STDERR>".format(failed_jobAttempt.STDERR_txt)
            )
            if numAttempts < self.max_reattempts:
                for f in os.listdir(task.job_output_dir):
                    if f != 'jobinfo':
                        p = opj(task.job_output_dir, f)
                        if os.path.isdir(p):
                            shutil.rmtree(p)
                        else:
                            os.unlink(p)
                self._run_task(task)
                return True
            else:
                self.log.warning("{0} of {1} failed, on attempt # {2}.\n".format(failed_jobAttempt, task, numAttempts)
                                 + "<COMMAND path=\"{1}\">\n{0}\n</COMMAND>\n".format(
                    failed_jobAttempt.get_command_shell_script_text(), failed_jobAttempt.command_script_path)
                                 + "<STDERR>\n{0}\n</STDERR>".format(failed_jobAttempt.STDERR_txt)
                )
                self.status = 'failed'
                self.save()
                return False


    def run(self, terminate_on_fail=True, finish=True):
        """
        Runs a workflow using the TaskGraph of jobs

        :param terminate_on_fail: (bool) If True, the workflow will self terminate of any of the tasks of this stage fail `max_job_attempts` times
        """
        self.log.info("Generating TaskGraph...")
        wfDAG = WorkflowManager(self)
        self.log.info("Running TaskGraph.")

        def run_ready_tasks():
            submitted_tasks = []
            ready_tasks = wfDAG.get_ready_tasks()

            cores_used = self.jobManager.jobAttempts.filter(queue_status='queued').aggregate(
                Sum('task__cpu_requirement')
            ).values()[0] or 0

            for task in ready_tasks:
                if self.max_cores > 0 and cores_used + task.cpu_requirement > self.max_cores:
                    # At max number of concurrent cores
                    self.log.info('Using {0}/{1} cores and next task requires {2} core(s), waiting.'.format(cores_used,
                                                                                                            self.max_cores,
                                                                                                            task.cpu_requirement))
                    return submitted_tasks

                self._run_task(task)
                submitted_tasks.append(task)
                wfDAG.queue_task(task)
                cores_used += task.cpu_requirement

            for st in submitted_tasks:
                if st.NOOP:
                    st._has_finished('NOOP')
                    wfDAG.complete_task(st)

            if submitted_tasks and len(submitted_tasks) == len(ready_tasks):
                # Keep going through DAG j case NOOPs were submitted
                run_ready_tasks()

            return submitted_tasks

        try:
            run_ready_tasks()
            for jobAttempt in self.jobManager.yield_all_queued_jobs():
                task = jobAttempt.task
                #self.log.info('Finished {0} for {1} of {2}'.format(jobAttempt,task,task.stage))
                if jobAttempt.successful or task.succeed_on_failure:
                    task._has_finished(jobAttempt)
                    wfDAG.complete_task(task)
                else:
                    if not self._reattempt_task(task, jobAttempt):
                        task._has_finished(jobAttempt) #job has failed and out of reattempts
                        if terminate_on_fail:
                            self.log.warning(
                                "{0} of {1} has reached max_reattempts and terminate_on_fail==True so terminating.".format(
                                    jobAttempt, task))
                            self.terminate()

                run_ready_tasks()

                if wfDAG.queue_is_empty():
                    self.log.info('No tasks left in the TaskGraph.')
                    break

            if finish:
                self.finished()
            return self

        except Exception as e:
            self.log.error(
                'An exception was raised during workflow execution, terminating workflow and then re-raising exception.')
            self.terminate(exit=False)
            raise

    def finished(self):
        """
        Call at the end of every workflow.

        """
        self.finished_on = timezone.now()
        self.save()
        self.log.info("Finished {0}, last stage's output dir: {1}".format(self,
                                                                          self.stages.order_by('-order_in_workflow')[
                                                                              0].output_dir))
    @property
    def successful(self):
        return self.stages.filter(successful=False).count() == 0

    def get_tasks_by(self, stage=None, tags={}, op="and"):
        """
        Returns the list of tasks that are tagged by the keys and vals in tags dictionary

        :param op: (str) either 'and' or 'or' as the logic to filter tags with
        :param tags: (dict) tags to filter for
        :returns: (queryset) a queryset of the filtered tasks

        >>> task.get_tools_by(op='or',tags={'color':'grey','color':'orange'})
        >>> task.get_tools_by(op='and',tags={'color':'grey','shape':'square'})
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
            for k, v in tags.items():
                tasks = tasks.filter(tasktag__key=k, tasktag__value=v)

            return tasks

    def get_task_by(self, tags={}, stage=None, op="and"):
        """
        Returns the list of tasks that are tagged by the keys and vals in tags dictionary.

        :raises Exception: if more or less than one task is returned

        :param op: (str) Choose either 'and' or 'or' as the logic to filter tags with
        :param tags: (dict) A dictionary of tags you'd like to filter for
        :returns: (queryset) a queryset of the filtered tasks

        >>> task.get_task_by(op='or',tags={'color':'grey','color':'orange'})
        >>> task.get_task_by(op='and',tags={'color':'grey','color':'orange'})
        """

        tasks = self.get_tasks_by(stage=stage, op=op,
                                  tags=tags) #there's just one group of tasks with this tag combination
        n = tasks.count()
        if n > 1:
            raise Exception("More than one task with tags {0} in {1}".format(tags, stage))
        elif n == 0:
            raise Exception("No tasks with with tags {0}.".format(tags))
        return tasks[0]

    def iter_as_dict(self):
        for s in self.stages:
            for t in s:
                for o in t.output_files:
                    yield s, t, o

    def __str__(self):
        return 'Workflow[{0}] {1}'.format(self.id, re.sub('_', ' ', self.name))

    def describe(self):
        return """output_dir: {0.output_dir}""".format(self)

    @models.permalink
    def url(self):
        return ('workflow_view', [str(self.id)])
