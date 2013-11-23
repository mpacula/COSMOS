from cosmos import session
import django.db.models

from Job.jobattempt import JobAttempt
if session.settings['DRM'] == 'local':
    from Job.jobmanager_local import JobManager
elif session.settings['DRM'] == 'Native_LSF':
    from Job.jobmanager_lsf import JobManager
else:
    from Job.jobmanager_drmaa import JobManager

from .Workflow.models import TaskFile, Workflow, WorkflowManager, Stage, TaskTag, TaskEdge, Task, TaskError, TaskValidationError, TaskFileValidationError, WorkflowError

__all__ = ['JobAttempt', 'JobManager', 'TaskFile', 'Workflow', 'WorkflowManager', 'Stage', 'TaskTag', 'TaskEdge', 'Task',
           'TaskError', 'TaskValidationError', 'TaskFileValidationError', 'WorkflowError']
