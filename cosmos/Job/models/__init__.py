from cosmos import session
import django.db.models

from jobattempt import JobAttempt
if session.settings['DRM'] == 'local':
    from jobmanager_local import JobManager
else:
    from jobmanager_drmaa import JobManager

__all__ = ['JobAttempt', 'JobManager']
