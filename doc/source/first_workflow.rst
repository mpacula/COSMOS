Writing Your First Workflow
===========================

Workflow Objects
________________


#. **Workflow** (:doc:`API </API/Workflow>`)
      .. autoclass:: Workflow.models.Workflow
#. **Batch** (:doc:`API </API/Batch>`)
      .. autoclass:: Workflow.models.Batch
#. **Node** (:doc:`API </API/Node>`)
      .. autoclass:: Workflow.models.Node
#. **JobAttempt** (:doc:`API </API/JobAttempt>`)   
      .. autoclass:: JobManager.models.JobAttempt


Hello World
___________

The simplest workflow you could have.  Edit :file:`firstflow.py`, and put the following into it.

.. code-block:: python
   :linenos:

   #these two lines go at the beggining of every workflow
   import cosmos_session
   from Workflow.models import Workflow
   
   workflow = Workflow.create('My First Workflow')
   first_batch = Workflow.add_batch('My First Batch')
   node = first_batch.add_node(name='My First Node', pcmd='echo "Hello World"')
   workflow.run_and_wait(first_batch)
   workflow.finished()

Then run:

.. code-block:: bash
   
   $ python firstflow.py
   
This will print `Hello World` to stdout.  You can get view the path and read the file of the job's STDOUT via the web interface,
or by calling ``node.get_successful_jobAttempt().get_drmaa_STDOUT_filepath()`` in the :doc:`/shell`.


