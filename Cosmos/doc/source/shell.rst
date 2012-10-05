Cosmos Shell
============

Using various Django features and apps, you can quickly enter an ipython shell with access to all your workflow objects.

.. note:: This is an advanced feature.

Launch the IPython shell
++++++++++++++++++++++++

.. code-block:: bash

   $ cosmos adm shell
 
You can then interactively investigate and perform all sorts of operations.  This is really powerful when interacting with the
:term:`Django` API, since all of the Cosmos objects are Django models. All of the Cosmos classes are automatically imported for you.

.. code-block:: python 

   >>> all_workflows = Workflow.objects.all()
   
   >>> workflow = all_workflows[2]
   
   >>> batch = workflow.batches[3]
   
   >>> batch.file_size
   
   >>> batch.nodes[3].get_successful_jobAttempt().queue_status
   

Interactive Workflow
++++++++++++++++++++

You can even run a workflow:

.. code-block:: python 

   >>> import cosmos_session
   >>> wf = Workflow.create("interactive workflow")
   >>> b = wf.add_batch("batch1")
   >>> b.add_node("hi","echo \"hello world\" > {output_dir}/out")
   >>> wf.run_and_wait(b)
   >>> wf.finished()
   