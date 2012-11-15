TODO
----
* create a cli for running a workflow so its not executed directly
* don't allow a user to add tasks to a successful stage without specifically setting a parameter.  Usually this is mistake caused by using the same name twice 
* capture all exceptions so that I can do a graceful exit
* prompt user with a 30s timeout if they're doing hard resets or restart_from_here()
* allow for stage deletes so hard_resets don't take forever
* remove workflow.resume_from_failure
* add drmaa.session.delete(jobtemplate)


Completed
+++++++++
* remove remote terminate.  remote termination can and should always be achieved via sending a ctrl+c signal
* capture ctrl+c



