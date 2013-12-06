from django.db import models

class TaskTag(models.Model):
    """
    A SQL row that duplicates the information of Task.tags that can be used for filtering, etc.
    """
    class Meta:
        app_label = 'cosmos'
        db_table = 'cosmos_tasktag'

    task = models.ForeignKey('cosmos.Task')
    key = models.CharField(max_length=63)
    value = models.CharField(max_length=255)

    def __str__(self):
        return "<TaskTag[self.id] {self.key}: {self.value} for Task[{task.id}]>".format(self=self, task=self.task)