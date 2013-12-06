from cosmos import session
import django.db.models
from job.JobAttempt import JobAttempt
if session.settings['DRM'] == 'local':
    from job.jobmanager_local import JobManager
elif session.settings['DRM'] == 'Native_LSF':
    from job.jobmanager_lsf import JobManager
else:
    from job.jobmanager_drmaa import JobManager


from .workflow.Workflow import Workflow
from .workflow import TaskValidationError, WorkflowError, TaskError
from .workflow.Stage import Stage
from .workflow.TaskTag import TaskTag
from .workflow.TaskFile import TaskFile, TaskFileValidationError
from .workflow.WorkflowManager import WorkflowManager
from .workflow.Task import Task


__all__ = ['JobAttempt', 'JobManager', 'TaskFile', 'Workflow', 'Stage', 'TaskTag', 'Task',
           'TaskError', 'TaskValidationError', 'TaskFileValidationError', 'WorkflowError']

