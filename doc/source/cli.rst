Command Line Interface
======================

The CLI can be accessed by typing :command:`cosmos` at your prompt.  See below for usage.

Examples
++++++++

Get Usage Help:

.. code-block:: bash

   $ cosmos -h

List workflows:

.. code-block:: bash

   $ cosmos wf list
   
Terminate workflow with id 1

.. code-block:: bash

   $ cosmos wf terminate 1


List the DRMAA specific jobids of the queued jobs in workflow with id 1:
.. code-block:: bash

   $ cosmos wf jobs -jid -q 1

Manually send a bkill to above jobs (note, Cosmos will reattempt a failed job 3 times by default):

.. code-block:: bash

   $ cosmos wf jobs -jid -q 1 |xargs bkill

.. automodule:: Cosmos.cli
   :members: