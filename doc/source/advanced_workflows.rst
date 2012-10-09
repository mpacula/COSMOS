Advanced Workflows
==================

.. note:: Under construction.

Tracking Outputs
________________

You should actually avoid using STDOUT to store the output of your jobs, and redirect it to a file.  Cosmos helps you keep track
of these output files using a dictionary you pass to :function:`Batch.add_node()` using the output parameter.  Time to really dig into the add_node() api:

.. automethod:: Workflow.models.Batch.add_node()
   :noindex:



.. code-block:: python
   :linenos:

   import cosmos_session
   from Workflow.models import Workflow
   import os
   
   WF = Workflow.start('My Advanced Workflow')
   
   B_one = WF.add_batch('echo')
   first_node = B_one.add_node(name='My First Node', pcmd='echo "Hello World"')
   WF.run_and_wait(first_batch)
   
   B_two = WF.add_batch('My Second Batch')
   for i in range(1,5):
       node1 = B_one.add_node(name='node %i'%i, pcmd='echo "Hello World #%s" % i')
   WF.run_and_wait(B_two)
   
   # Finish the workflow; every workflow ends with this command
   WF.finished()  