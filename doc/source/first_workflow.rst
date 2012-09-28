Writing Your First Workflow
===========================

Workflow Objects
________________


#. **Workflow** (:doc:`API </API/Workflow>`)
      .. autoclass:: Workflow.models.Workflow
         :noindex:
#. **Batch** (:doc:`API </API/Batch>`)
      .. autoclass:: Workflow.models.Batch
         :noindex:
#. **Node** (:doc:`API </API/Node>`)
      .. autoclass:: Workflow.models.Node
         :noindex:
#. **JobAttempt** (:doc:`API </API/JobAttempt>`)   
      .. autoclass:: JobManager.models.JobAttempt
         :noindex:


Hello World
___________

The simplest workflow you could have.  Edit :file:`firstflow.py`, and put the following into it.

.. code-block:: python
   :linenos:

   # These two lines go at the beggining of every workflow
   import cosmos_session
   from Workflow.models import Workflow
   
   # Create workflow
   my_workflow = Workflow.create('My First Workflow')
   
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

Cosmos will keep track of output files for you, if you want it to.  Open up :file:`firstflow.py` again, and make these changes:

* Call :function:`resume` on the workflow instead of create.
* Rename some variables to get more succinct code

.. automethod:: Workflow.models.Workflow.add_batch()
   :noindex:
   
.. automethod:: Workflow.models.Batch.add_node()
   :noindex:
   
* When you run a batch or node that has already been successful

.. code-block:: python
   :linenos:

   import cosmos_session
   from Workflow.models import Workflow
   
   WF = Workflow.resume('My First Workflow')
   
   B_one = WF.add_batch('My First Batch')
   first_node = B_one.add_node(name='My First Node', pcmd='echo "Hello World"')
   WF.run_and_wait(first_batch)  
   
   batch2 = my_
   
   # Finish the workflow; every workflow ends with this command
   WF.finished()  

