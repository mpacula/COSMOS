.. _cli:

Command Line Interface
______________________

.. code-block:: bash

    $ cosmos -h
    usage: cosmos [-h] <command> ...

    Cosmos CLI

    optional arguments:
      -h, --help  show this help message and exit

    Commands:
      <command>
        resetdb   DELETE ALL DATA in the database and then run a syncdb
        shell     Open up an ipython shell with Cosmos objects preloaded
        syncdb    Sets up the SQL database
        list      List all workflows
        runweb    Start the webserver

         
.. note:: When listing objects, the number beside each Workflow inside brackets,
`[#]`, is the sql ID of that object.

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

   $ cosmos resetdb

List workflows
++++++++++++++
.. code-block:: bash

   $ cosmos ls


API
___

.. automodule:: cosmos.cli
   :members: