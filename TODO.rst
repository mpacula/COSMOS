TODO
----
* create a cli for running a workflow so its not executed directly
* capture all exceptions so that I can do a graceful exit


Completed
+++++++++
* remove remote terminate.  remote termination can and should always be achieved via sending a ctrl+c signal
* capture ctrl+c
* prompt user with a 30s timeout if they're doing hard resets or restart_from_here()
* bulk delete
* remove workflow.resume_from_failure
* add drmaa.session.delete(jobtemplate)


