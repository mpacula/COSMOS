.. _config:

Configuration
=============

1. Install Configuration File
_______________________________

Type ``cosmos`` at the command line, and generate a default configuration file in :file:`~/.cosmos/config.ini`.
Edit :file:`~/.cosmos/config.ini`, and configure it to your liking; the instructions are in the file, but copied
below for reference.

.. literalinclude:: ../../cosmos/default_config.ini

.. _local:

Local Development and Testing
******************************

Setting DRM = local in your config file will cause jobs to be submitted as background
processes on the local machine using :py:mod:`subprocess`.Popen.

Be careful how many resource intensive jobs your workflow submits at once when using `DRM = local`.  If the workflow
is large, be sure to specify :param:`max_cores` when calling :meth:`cosmos.Workflow.start`.


2. Create SQL Tables and Load Static Files
__________________________________________

Once you've configured Cosmos, setting up the SQL database tables is easy.  The web interface is a
:term:`Django` application, which requires you to run the collectstatic command.  This moves all the necessary images, css, and
javascript files to the ~/.cosmos/static/ directory.  Run these two commands after you've configured the database in the
cosmos configuration file.

.. code-block:: bash

   $ cosmos syncdb
   $ cosmos collectstatic

If you ever switch to a different database in your :file:`~/.cosmos/config.ini`, be sure to run `cosmos syncdb`
to recreate your tables.

You can always completely reset the database with the command ``cosmos resetdb``