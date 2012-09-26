Tutorial
========

We'll start by running a simple test workflow

Command Line Interface
______________________

Make sure your environment variables, are set.  In particular ``/path/to/Cosmos/bin`` must be in your PATH
Use the shell command ``env`` if you're not sure what's in your environment.

.. code-block:: bash

   $ cosmos -h
   usage: cli.py [-h] {adm,wf} ...
   
   positional arguments:
     {adm,wf}
       adm       Admin
       wf        Workflow
   
   optional arguments:
     -h, --help  show this help message and exit
         
Explore the available commands, if you wish.


Execute the test workflow
_________________________

.. code-block:: bash

   $ python /path/to/cosmos/my_workflows/testflow.py
   
The console will generate a lot of output as the workflow runs, and you can view the results
of the workflow in the web interface. 


Launch the Web Interface
________________________

.. code-block:: bash

   $ cosmos adm runweb
   
Visit http://your-ip:8080 to access it.   

.. figure:: imgs/webinterface.png
   :width: 800px
   :align: center
   
   Viewing the test workflow in the web interface.

