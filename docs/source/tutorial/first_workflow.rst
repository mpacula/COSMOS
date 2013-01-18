.. _first_workflow:

Writing Your First Workflow
===========================

Hello World
___________

Here is the source code of the :file:`example_workflows/ex1.py` you ran in :ref:`getting_started`.

.. literalinclude:: ../literalincludes/example_workflows/ex1.py
   :linenos: 

Here's the job dependency graph that was created:

.. figure:: /imgs/ex1.svg
   :width: 100%
   :align: center 

Reload a Workflow
_________________

You can add more stages to the workflow, without re-running tasks that were already successful.  An example is in :file:`example_workflows/ex1_b.py`.

.. literalinclude:: ../literalincludes/mexample_workflows/ex1_b.py
   :linenos: 

Run it with the command:

.. code-block:: bash

   $ python ex1_b.py
