Writing Your First Workflow
===========================

Primary Workflow Objects
________________________


#. **Workflow** (:doc:`API </API/Workflow>`)
      .. autoclass:: Workflow.models.Workflow
         :noindex:
#. **Batch** (:doc:`API </API/Workflow>`)
      .. autoclass:: Workflow.models.Batch
         :noindex:
#. **Node** (:doc:`API </API/Workflow>`)
      .. autoclass:: Workflow.models.Node
         :noindex:
#. **JobAttempt** (:doc:`API </API/JobManager>`)   
      .. autoclass:: JobManager.models.JobAttempt
         :noindex:


Hello World
___________

The simplest workflow you could have.  Edit :file:`firstflow.py`, and put the following into it.

.. code-block:: python
   :linenos:

   # These two lines go at the beggining of every workflow
   import cosmos.session
   from Workflow.models import Workflow
   
   # Create workflow
   my_workflow = Workflow.start('My First Workflow')
   
   # Create batch and node
   first_batch = my_workflow.add_batch('My First Batch')
   first_node = first_batch.add_node(name='My First Node', pcmd='echo "Hello World"')
   
   # Run the the jobs in this batch, and wait for them to finish
   my_workflow.run_and_wait(first_batch)  
   
   # Finish the workflow; every workflow ends with this command
   my_workflow.finished()  

Then run:

.. code-block:: bash
   
   $ python firstflow.py
   
This will print `Hello World` to stdout.  You can get view the path and read the file of the job's STDOUT via the web interface,
or by calling ``node.get_successful_jobAttempt().get_drmaa_STDOUT_filepath()`` in the :doc:`/shell`.

Resume a Workflow
_________________

First, get familiar with the APIs of these functions, especially their first few parameters:

.. automethod:: Workflow.models.Workflow.start
   :noindex:

.. automethod:: Workflow.models.Workflow.add_batch
   :noindex:
   
.. automethod:: Workflow.models.Batch.add_node
   :noindex:

So with the code changes below, Cosmos will resume the workflow and skip the first batch since it was already successful.  It will then
run 5 jobs in the second batch.

.. code-block:: python
   :linenos:

   import cosmos.session
   from Workflow.models import Workflow
   import os
   
   WF = Workflow.start('My First Workflow')
   
   B_one = WF.add_batch('My First Batch')
   first_node = B_one.add_node(name='My First Node', pcmd='echo "Hello World"')
   WF.run_and_wait(first_batch)
   
   B_two = WF.add_batch('My Second Batch')
   for i in range(1,5):
       node1 = B_one.add_node(name='node %i'%i, pcmd='echo "Hello World #%s" % i')
   WF.run_and_wait(B_two)
   
   # Finish the workflow; every workflow ends with this command
   WF.finished()  
