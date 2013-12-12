import os
import re
from django.db import models, transaction
from django.db.models import Q
from cosmos.models import JobAttempt
from cosmos.utils.helpers import validate_name, check_and_create_output_dir, folder_size
from cosmos.utils import helpers
from django.core.exceptions import ValidationError
from django.utils import timezone
import signals
from cosmos.templatetags import extras
from ordereddict import OrderedDict

from .Task import Task
from .TaskTag import TaskTag
from .TaskFile import TaskFile
from . import status_choices
from .. import session
from ...utils.helpers import validate_not_null

class Stage(models.Model):
    """
    A group of jobs that can be run independently.  See `Embarassingly Parallel <http://en.wikipedia.org/wiki/Embarrassingly_parallel>`_ .

    .. note:: A Stage should not be directly instantiated, use :py:func:`Workflow.models.Workflow.add_stage` to create a new stage.
    """

    class Meta:
        unique_together = (('name', 'workflow'))
        app_label = 'cosmos'
        db_table = 'cosmos_stage'

    workflow = models.ForeignKey('cosmos.Workflow')
    name = models.CharField(max_length=200)
    order_in_workflow = models.IntegerField(null=True)
    status = models.CharField(max_length=200, choices=status_choices, default='no_attempt')
    successful = models.BooleanField(default=False)
    started_on = models.DateTimeField(null=True, default=None)
    created_on = models.DateTimeField(null=True, default=None)
    finished_on = models.DateTimeField(null=True, default=None)
    output_dir = models.CharField(max_length=255, default=None, null=True)


    def __init__(self, *args, **kwargs):
        kwargs['created_on'] = timezone.now()
        super(Stage, self).__init__(*args, **kwargs)

        validate_not_null(self.workflow)
        validate_name(self.name, 'name')

        if hasattr(session, 'stage_output_dir'):
            self.output_dir = session.stage_output_dir(self)
        else:
            self.output_dir = os.path.join(self.workflow.output_dir)

        #check_and_create_output_dir(self.output_dir)

    def set_status(self, new_status, save=True):
        "Set Stage status"
        self.log.info('{0} {1}'.format(self, new_status))
        self.status = new_status

        if new_status == 'successful':
            self.successful = True

        if save: self.save()

    @property
    def log(self):
        return self.workflow.log

    @property
    def percent_done(self):
        """
        Percent of tasks that have completed
        """
        done = Task.objects.filter(stage=self, successful=True).count()
        if self.num_tasks == 0 or done == 0:
            if self.status == 'in_progress' or self.status == 'failed':
                return 1
            return 0
        r = int(100 * float(done) / float(self.num_tasks))
        return r if r > 1 else 1

    def failed_jobAttempts(self):
        return JobAttempt.objects.filter(task__in=self.tasks, queue_status='finished', successful=False)


    def get_stats(self):
        """
        :param: a list of 3-tuples of format (title,field,statistic)
        :return: (dict) of stats about jobs
        """
        stats_to_get = [('avg_percent_cpu', 'percent_cpu', 'Avg', extras.format_percent),
                        ('avg_wall_time', 'wall_time', 'Avg', extras.format_time),
                        ('max_wall_time', 'wall_time', 'Max', extras.format_time),
                        ('avg_block_io_delays', 'block_io_delays', 'Avg', extras.format_time),
                        ('avg_rss_mem', 'avg_rss_mem', 'Avg', extras.format_memory_kb),
                        ('max_rss_mem', 'max_rss_mem', 'Max', extras.format_memory_kb),
                        ('avg_virtual_mem', 'avg_virtual_mem', 'Avg', extras.format_memory_kb)
        ]
        stat_names = [s[0] for s in stats_to_get]
        aggregate_kwargs = {}
        for title, field, statistic, formatfxn in stats_to_get:
            if statistic not in ['Avg', 'Sum', 'Max', 'Min', 'Count']:
                raise ValidationError('Statistic {0} not supported'.format(statistic))
            aggr_fxn = getattr(models, statistic)
            aggregate_kwargs[title] = aggr_fxn(field)
        r = self.successful_jobAttempts.aggregate(**aggregate_kwargs)
        d = OrderedDict()
        for title, field, stat, formatfxn in stats_to_get:
            d[title] = formatfxn(r[title])
        return d


    #TODO deprecated
    def get_sjob_stat(self, field, statistic):
        """
        Aggregates a task successful job's field using a statistic.
        :param field: (str) name of a tasks's field.  ex: wall_time or avg_rss_mem
        :param statistic: (str) choose from ['Avg','Sum','Max','Min','Count']

        >>> stage.get_stat('wall_time','Avg')
        120
        """

        if statistic not in ['Avg', 'Sum', 'Max', 'Min', 'Count']:
            raise ValidationError('Statistic {0} not supported'.format(statistic))
        aggr_fxn = getattr(models, statistic)
        aggr_field = '{0}__{1}'.format(field, statistic.lower())
        return self.successful_jobAttempts.aggregate(aggr_fxn(field))[aggr_field]

    @property
    def successful_jobAttempts(self):
        return JobAttempt.objects.filter(successful=True, task__in=Task.objects.filter(stage=self))

    def get_task_stat(self, field, statistic):
        """
        Aggregates a task's field using a statistic
        :param field: (str) name of a tasks's field.  ex: cpu_req, mem_req
        :param statistic: (str) choose from ['Avg','Sum','Max','Min','Count']

        >>> stage.get_stat('cpu_requirement','Avg')
        120
        """

        if statistic not in ['Avg', 'Sum', 'Max', 'Min', 'Count']:
            raise ValidationError('Statistic {0} not supported'.format(statistic))
        aggr_fxn = getattr(models, statistic)
        aggr_field = '{0}__{1}'.format(field, statistic.lower())
        r = Task.objects.filter(stage=self).aggregate(aggr_fxn(field))[aggr_field]
        return int(r) if r or r == 0.0 else r


    @property
    def file_size(self, human_readable=True):
        "Size of the stage's output_dir"
        return folder_size(self.output_dir, human_readable=human_readable)

    @property
    def wall_time(self):
        """Time between this stage's creation and finished datetimes.  Note, this is a timedelta instance, not seconds"""
        return self.finished_on.replace(microsecond=0) - self.started_on.replace(
            microsecond=0) if self.finished_on else timezone.now().replace(microsecond=0) - self.started_on.replace(
            microsecond=0)

    @property
    def tasks(self):
        "Queryset of this stage's tasks"
        return Task.objects.filter(stage=self)

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
        return Task.objects.filter(stage=self, successful=True).count()

    def get_all_tag_keys_used(self):
        """Returns a set of all the keyword tags used on any task in this stage"""
        try:
            return self.tasks.all()[0].tags.keys()
        except IndexError:
            return {}
        except AttributeError:
            return set(map(lambda x: x['key'], TaskTag.objects.filter(task__in=self.tasks).values('key').distinct()))

    def yield_task_resource_usage(self):
        """
        :yields: (list of tuples) tuples contain resource usage and tags of all tasks.  The first element is the name, the second is the value.
        """
        #TODO rework with time fields
        for task in self.tasks:
            sja = task.get_successful_jobAttempt()
            if sja:
                yield [jru for jru in
                       sja.resource_usage_short] + task.tags.items() #add in tags to resource usage tuples

    # def add_task(self, pcmd, tags={}, **kwargs):
    #     """
    #     Creates a new task for this stage, and saves it.
    #     If a task with `tags` already exists in this stage, just return it.
    #     Has the same signature as :meth:`Task.__init__` minus the stage argument.
    #
    #     :returns: The task added.
    #     """
    #     q = Task.objects.filter(stage=self,tags=tags)
    #     # if q.count() > 0:
    #     #     return q.all()[0]
    #
    #     return Task.create(stage=self,pcmd=pcmd,**kwargs)

    def is_done(self):
        """
        :returns: True if this stage is finished successfully or failed, else False
        """
        return self.status == 'successful' or self.status == 'failed'

    def _are_all_tasks_done(self):
        """
        :returns: True if all tasks have succeeded or failed in this stage, else False
        """
        return self.tasks.filter(Q(status='successful') | Q(status='failed')).count() == self.tasks.count()

    def _has_finished(self):
        """
        Executed when this stage has completed running.
        All it does is sets status as either failed or successful
        """
        num_tasks = Task.objects.filter(stage=self).count()
        num_tasks_successful = self.num_tasks_successful
        num_tasks_failed = Task.objects.filter(stage=self, status='failed').count()

        if num_tasks_successful == num_tasks:
            self.set_status('successful')
        elif num_tasks_failed + num_tasks_successful == num_tasks:
            self.set_status('failed')
        else:
            raise Exception('Stage._has_finished() called, but not all tasks are completed.')

        self.finished_on = timezone.now()
        self.save()
        signals.stage_status_change.send(sender=self, status=self.status)

    def get_tasks_by(self, tags={}, op='and'):
        """
        An alias for :func:`Workflow.get_tasks_by` with stage=self

        :returns: a queryset of filtered tasks
        """
        return self.workflow.get_tasks_by(stage=self, tags=tags, op=op)

    def get_task_by(self, tags={}, op='and'):
        """
        An alias for :func:`Workflow.get_task_by` with stage=self

        :returns: a queryset of filtered tasks
        """
        return self.workflow.get_task_by(stage=self, op=op, tags=tags)

    def group_tasks_by(self, keys=[]):
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
            yield {}, self.tasks
        else:
            task_tag_values = TaskTag.objects.filter(task__in=self.tasks, key__in=keys).values() #get this stage's tags
            #filter out any tasks without all keys

            task_id2tags = {}
            for task_id, ntv in helpers.groupby(task_tag_values, lambda x: x['task_id']):
                task_tags = dict([(n['key'], n['value']) for n in ntv])
                task_id2tags[task_id] = task_tags

            for tags, task_id_and_tags_tuple in helpers.groupby(task_id2tags.items(), lambda x: x[1]):
                task_ids = [x[0] for x in task_id_and_tags_tuple]
                yield tags, Task.objects.filter(pk__in=task_ids)

    @transaction.commit_on_success
    def delete(self, *args, **kwargs):
        """
        Bulk deletes this stage and all files associated with it.
        """
        self.log.info('Deleting Stage {0}.'.format(self.name))
        # if os.path.exists(self.output_dir):
        #     self.log.info('Deleting directory {0}...'.format(self.output_dir))
        #     os.system('rm -rf {0}'.format(self.output_dir))
        self.workflow.bulk_delete_tasks(self.tasks)
        super(Stage, self).delete(*args, **kwargs)
        self.log.info('{0} Deleted.'.format(self))


    @models.permalink
    def url(self):
        "The URL of this stage"
        return ('stage_view', [str(self.workflow.id), self.name])

    def __str__(self):
        return '<Stage[{0}] {1}>'.format(self.id, self.name)
