import os
import re
import hashlib
from django.db import models
from cosmos.utils.helpers import folder_size
import shutil
from urllib import pathname2url

class TaskFileValidationError(Exception): pass


class TaskFileError(Exception): pass


i = 0


def get_tmp_id():
    global i
    i += 1
    return i


class TaskFile(models.Model, object):
    """
    Task File
    """

    class Meta:
        app_label = 'cosmos'
        db_table = 'cosmos_taskfile'

    name = models.CharField(max_length=50, null=True)
    basename = models.CharField(max_length=50, null=True)
    fmt = models.CharField(max_length=30, null=True) #file format
    path = models.CharField(max_length=250, null=True)
    persist = models.BooleanField(default=False)
    deleted_because_intermediate = models.BooleanField(default=False)
    must_exist = models.BooleanField(default=True)


    def __init__(self, *args, **kwargs):
        """
        :param name: This is the name of the file, and is used as the key for obtaining it.  No Tool an
            have multiple TaskFiles with the same name.  Defaults to ``fmt``.
        :param fmt: The format of the file.  Defaults to the extension of ``path``.
        :param path: The path to the file.  Required.
        :param basename: (str) The name to use for the file for auto-generated paths.  You must explicitly
            specify the extension of the filename, if you want one i.e. 'myfile.txt' not 'myfile'
        :param persist: (bool) If True, this file will not be deleted even if it is an intermediate
            file, and workflow.delete_intermediates is turned on.  Defaults to False.
        """
        super(TaskFile, self).__init__(*args, **kwargs)

        if not self.fmt and self.path:
            try:
                groups = re.search('\.([^\.]+)$', self.path).groups()
                self.fmt = groups[0]

            except AttributeError as e:
                raise AttributeError('{0}. Probably malformed path: {1}'.format(e, self.path))

        if not self.name and self.fmt:
            self.name = self.fmt
        if not self.fmt and self.name:
            self.fmt = self.name

        self.tmp_id = get_tmp_id()

        if not re.search("^[\w\.]+$", self.name):
            raise TaskFileValidationError, 'The taskfile.name can only contain letters, numbers, and periods. Failed name is "{0}"'.format(
                self.name)

    @property
    def workflow(self):
        return self.task.workflow

    @property
    def task(self):
        "The task this TaskFile is an output for"
        return self.task_output_set.get()

    @property
    def file_size(self, human_readable=True):
        "Size of the taskfile's output_dir"
        return folder_size(self.path, human_readable=human_readable) or 'NA'


    @property
    def sha1sum(self):
        return hashlib.sha1(file(self.path).read())

    def __str__(self):
        return "#F[{0}:{1}:{2}]".format(self.id if self.id else 't_{0}'.format(self.tmp_id), self.name, self.path)

    @models.permalink
    def url(self):
        return ('taskfile_view', [str(self.id)])

    def delete_because_intermediate(self):
        """
        Deletes this file and marks it as deleted because it is an intermediate file.
        """
        if not self.persist:
            self.workflow.log.info('Deleting Intermediate file {0}'.format(self.path))
            self.deleted_because_intermediate = True
            if os.path.isdir(self.path):
                os.system('rm -rf {0}'.format(os.path.join(self.path, '*')))
            else:
                os.system('echo "" > {0}'.format(self.path)) # overwrite with empty file
            self.save()
        else:
            raise TaskFileError, "{0} should not be deleted because persist=True".format(self)

    def delete(self, *args, **kwargs):
        """
        Deletes this task and all files associated with it
        """
        shutil.rmtree(self.path)
        super(TaskFile, self).delete(*args, **kwargs)
