.. _simple_workflows:

Hello World
___________

Here is the source code of the :file:`example_workflows/ex1.py` you ran in :ref:`getting_started`.

.. literalinclude:: ../../../example_workflows/ex1.py

Here's the job dependency graph that was created:

.. figure:: /imgs/ex1.svg
   :width: 100%
   :align: center 

Reload a Workflow
_________________

You can add more stages to the workflow, without re-running tasks that were already successful.  An example is in :file:`example_workflows/ex1_b.py`.

.. literalinclude:: ../../../example_workflows/ex1_b.py

Run it with the command:

.. code-block:: bash

   $ python ex1_b.py
