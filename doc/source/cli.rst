.. _cli:

Command Line Interface
______________________

Make sure your environment variables are properly set (see :ref:`config`).
Use the shell command :command:`env` if you're not sure what's in your environment.

.. code-block:: bash

   $ cosmos -h
   usage: cli.py [-h] {adm,wf} ...
   
   positional arguments:
     {adm,wf}
       adm       Admin
       wf        Workflow
   
   optional arguments:
     -h, --help  show this help message and exit
         
Explore the available commands, using -h if you wish.  Or see the :ref:`cli` for more info.  Note that when
listing workflows, the number beside each Workflow inside brackets, `[#]`, is the ID of that object.

Examples
________

Get Usage Help:
+++++++++++++++
.. code-block:: bash

   $ cosmos -h
   
Reset the SQL database
++++++++++++++++++++++
.. note:: This will *not* delete the files associated with workflow output.

.. code-block:: bash

   $ cosmos adm resetentiredb

List workflows
++++++++++++++
.. code-block:: bash

   $ cosmos wf list


List the DRMAA specific jobids of the queued jobs in workflow with id 1
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
.. code-block:: bash

   $ cosmos wf jobs -jid -q 1

Manually send a bkill to above jobs (note, Cosmos will reattempt a failed job 3 times by default)
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
.. code-block:: bash

   $ cosmos wf jobs -jid -q 1 |xargs bkill

API
___

.. automodule:: cosmos.cli
   :members: