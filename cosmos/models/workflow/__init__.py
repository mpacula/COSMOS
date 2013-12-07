class TaskError(Exception): pass

class TaskValidationError(Exception): pass

class WorkflowError(Exception): pass

status_choices = (
    ('successful', 'Successful'),
    ('no_attempt', 'No Attempt'),
    ('in_progress', 'In Progress'),
    ('failed', 'Failed')
)