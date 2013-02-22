.. _shell:

Cosmos Shell
============

Using various Django features and apps, you can quickly enter an ipython shell with access to all your workflow objects.

.. note:: This is an advanced feature.

Launch the IPython shell
++++++++++++++++++++++++

.. code-block:: bash

   $ cosmos shell
 
You can then interactively investigate and perform all sorts of operations.
This is really powerful when interacting with the
:term:`Django` API, since most of the Cosmos objects are Django models.
Most Cosmos classes are automatically imported for you.

.. code-block:: python 

   all_workflows = Workflow.objects.all()
   workflow = all_workflows[2]
   stage = workflow.stages[3]
   stage.file_size
   stage.tasks[3].get_successful_jobAttempt().queue_status
   

Interactive Workflow
++++++++++++++++++++

You can even run a workflow:

.. code-block:: python 

    wf = Workflow.start('Interactive')
    stage = wf.add_stage('My Stage')
    task = stage.add_task('echo "hi"')
    wf.run()
    wf.finished()
   