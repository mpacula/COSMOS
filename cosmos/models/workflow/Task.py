import os
from django.db import models, transaction
from cosmos.utils.helpers import check_and_create_output_dir, folder_size
from django.core.exceptions import ValidationError
from picklefield.fields import PickledObjectField
from django.utils import timezone
import signals
from .TaskFile import TaskFile
from .TaskTag import TaskTag
from . import status_choices
from .. import session
opj=os.path.join

class Task(models.Model):
    """
    The object that represents the command line that gets executed.

    tags must be unique for all tasks in the same stage
    """
    class Meta:
        app_label = 'cosmos'
        db_table = 'cosmos_task'
        unique_together = (('tags','stage'))

    output_dir = models.CharField(max_length=255, default=None, null=True)
    always_local = models.BooleanField(default=False, help_text="Always run this task as a subprocess, even if DRM is not local", null=False)
    pcmd = models.TextField(
        help_text='Preformatted command.  almost always will contain special strings for TaskFiles which will later be replaced by their proper system path at execution')
    exec_command = models.TextField(help_text='The actual command that is executed', null=True)
    memory_requirement = models.IntegerField(help_text="Memory to reserve for jobs in MB", default=None, null=True)
    cpu_requirement = models.SmallIntegerField(help_text="Number of CPUs to reserve for this job", default=None,
                                               null=True)
    time_requirement = models.IntegerField(
        help_text="Time required to run in minutes.  If a job runs longer it may be automatically killed.",
        default=None, null=True)
    successful = models.BooleanField(default=False,
                                     help_text="True if the task has been executed successfully, else False")
    status = models.CharField(max_length=100, choices=status_choices, default='no_attempt')
    status_details = models.CharField(max_length=100, default='',
                                      help_text='Extra information about this task\'s status')
    NOOP = models.BooleanField(default=False,
                               help_text="No operation.  Likely used to store an input file, this task is not meant to be executed.")
    succeed_on_failure = models.BooleanField(default=False,
                                             help_text="If True, Task will succeed and workflow will progress even if its JobAttempts fail.")
    # cleared_output_files = models.BooleanField(default=False,help_text="If True, output files have been deleted/cleared.")
    # dont_delete_output_files = models.BooleanField(default=False,help_text="If True, prevents output files from being deleted even when this task becomes an intermediate and workflow.delete_intermediates == True.")

    _parents = models.ManyToManyField('cosmos.Task', related_name='_children')
    stage = models.ForeignKey('cosmos.Stage', help_text="The stage this task belongs to.")

    tags = PickledObjectField(null=False, default={})
    #on_success = PickledObjectField(null=False)
    created_on = models.DateTimeField(null=True, default=None)
    finished_on = models.DateTimeField(null=True, default=None)

    _output_files = models.ManyToManyField('cosmos.TaskFile', related_name='task_output_set', null=True,
                                           default=None)
    _input_files = models.ManyToManyField(TaskFile, related_name='task_input_set', null=True, default=None)

    @property
    def output_files(self):
        return self._output_files.all()

    @property
    def input_files(self):
        return self._input_files.all()


    def __init__(self, *args, **kwargs):
        """
        :param stage: (Stage) The stage this task is a part of.
        :param pcmd: (str) The preformatted command to execute. Usually includes strings that represent TaskFiles which will be automatically parsed. Required.
        :param tags: (dict) A dictionary keys and values to tag the task with. These tags can later be used by methods such as :py:meth:`~Workflow.models.stage.group_tasks_by` and :py:meth:`~Workflow.models.stage.get_tasks_by` Optional.
        :param on_success: (method) A method to run when this task succeeds.  Method is called with one parameter named 'task', the successful task.
        :param memory_requirement: (int) How much memory to reserve for this task in MB. Optional.
        :param cpu_requirement: (int) How many CPUs to reserve for this task. Optional.
        :param time_requirement: (int) Time required in miinutes.  If a job exceeds this requirement, it will likely be killed.
        :param NOOP: (booean) No Operation, this task does not get executed.
        :param succeed_on_failure: (booean) Succeed even if JobAttempts fails.
        :param dont_delete_output_files: (boolean) Prevents output files from being deleted, even when this task becomes an intermediate.
        :param hard_reset: (bool) Deletes this task and all associated files and start it fresh. Optional.
        :returns: A new task instance.  The instance has not been saved to the database.
        """
        kwargs['created_on'] = timezone.now()
        super(Task, self).__init__(*args, **kwargs)

        if self.output_dir is None:
            if hasattr(session, 'task_output_dir'):
                self.output_dir = session.task_output_dir(self)
            else:
                basedir = self.tags_as_query_string().replace('&', '__').replace('=', '_')
                self.output_dir = opj(self.stage.output_dir, basedir)


        # if len(self.tags) == 0:
        #     raise TaskValidationError, '{0} has no tags, at least one tag is required'.format(self)

    @staticmethod
    def create(stage, pcmd, **kwargs):
        """
        Creates a task.
        """
        task = Task(stage=stage, pcmd=pcmd, **kwargs)

        if Task.objects.filter(stage=task.stage, tags=task.tags).count() > 0:
            task.delete()
            raise ValidationError("Tasks belonging to a stage with the same tags detected! tags: {0}".format(task.tags))

        task.save()

        check_and_create_output_dir(task.output_dir)
        check_and_create_output_dir(task.job_output_dir)

        #Create task tags
        for key, value in task.tags.items():
            TaskTag.objects.create(task=task, key=key, value=value)

        return task

    @property
    def workflow(self):
        "This task's workflow"
        return self.stage.workflow

    @property
    def parents(self):
        "This task's parents"
        return self._parents.all()

    @property
    def task_tags(self):
        return TaskTag.objects.filter(task=self)

    @property
    def log(self):
        "This task's workflow's log"
        return self.workflow.log

    @property
    def file_size(self, human_readable=True):
        "Task filesize"
        return folder_size(self.output_dir, human_readable=human_readable)

    @property
    def output_file_size(self, human_readable=True):
        "Task filesize"
        return folder_size(self.job_output_dir, human_readable=human_readable)

    @property
    def job_output_dir(self):
        """Where the job output goes"""
        return os.path.join(self.output_dir)

    # @property
    # def output_paths(self):
    #     "Dict of this task's outputs appended to this task's output_dir."
    #     r = {}
    #     for key,val in self.outputs.items():
    #         r[key] = os.path.join(self.job_output_dir,val)
    #     return r

    @property
    def jobAttempts(self):
        "Queryset of this task's jobAttempts."
        return self.jobattempt_set.all().order_by('id')

    @property
    def wall_time(self):
        "Task's wall_time"
        return self.get_successful_jobAttempt().wall_time if self.successful else None

    def numAttempts(self):
        "This task's number of job attempts."
        return self.jobattempt_set.count()

    def get_successful_jobAttempt(self):
        """
        Get this task's successful job attempt.

        :return: this task's successful job attempt.  If there were no successful job attempts, returns None
        """
        jobs = self.jobattempt_set.filter(successful=True)
        if len(jobs) == 1:
            return jobs[0]
        elif len(jobs) > 1:
            raise Exception('more than 1 successful job, something went wrong!')
        else:
            return None # no successful jobs

    def set_status(self, new_status, save=True):
        "Set Task's status"
        self.status = new_status

        if new_status == 'successful':
            self.successful = True
            self.log.info('{0} successful!'.format(self))

        if save: self.save()

    def _has_finished(self, jobAttempt):
        """
        Should be executed whenever this task finishes.

        Sets self.status to 'successful' or 'failed' and self.finished_on to 'current_timezone'
        Will also run self.stage._has_finished() if all tasks in the stage are done.
        """

        if (
                        jobAttempt == 'NOOP'
                or jobAttempt.task.succeed_on_failure
            or self.jobattempt_set.filter(successful=True).count()
        ):
            self.set_status('successful')
        else:
            self.set_status('failed')

        self.finished_on = timezone.now()
        signals.task_status_change.send(sender=self, status=self.status)
        if self.stage._are_all_tasks_done(): self.stage._has_finished()

    def tag(self, **kwargs):
        """
        Tag this task with key value pairs.  If the key already exists, its value will be overwritten.

        >>> task.tag(color="blue",shape="circle")
        """
        #TODO don't allow tags called things like 'status' or other task attributes
        for key, value in kwargs.items():
            value = str(value)
            tasktag, created = TaskTag.objects.get_or_create(task=self, key=key, defaults={'value': value})
            if not created:
                tasktag.value = value
            tasktag.save()
            self.tags[key] = value

    def clear_job_output_dir(self):
        """
        Removes all files in this task's output directory
        """
        for otf in self.output_files:
            if not otf.persist and not otf.deleted_because_intermediate:
                otf.delete_because_intermediate()

    @models.permalink
    def url(self):
        "This task's url."
        return ('task_view', [str(self.workflow.id), self.stage.name, self.tags_as_query_string()])

    def tags_as_query_string(self):
        """
        Returns a string of tag keys and values as a url query string
        """
        import urllib

        return urllib.urlencode(self.tags)

    @transaction.commit_on_success
    def delete(self, *args, **kwargs):
        """
        Deletes this task and all files associated with it
        """
        self.log.info('Deleting {0} and it\'s output directory {1}'.format(self, self.output_dir))
        #todo delete stuff in output_paths that may be extra files
        for ja in self.jobattempt_set.all(): ja.delete()
        self.task_tags.delete()
        self.output_files.delete()
        if os.path.exists(self.output_dir):
            os.system('rm -rf {0}'.format(self.output_dir))
        super(Task, self).delete(*args, **kwargs)

    def __str__(self):
        return '<Task[{1}] {0} {2}>'.format(self.stage, self.id, self.tags)

