.. _config:

Configuration
=============

1. Setup Your Shell Environment
_______________________________

Type `cosmos` at the command line, and generate a default configuration file in :file:`~/.cosmos/config.ini`.
Edit :file:`~/.cosmos/config.ini`, and configure it to your liking.  There are only a few variables to set.

SGE specific environment variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You may also need to set (if they're not already) normal :term:`SGE` job submission variables such as:

.. code-block:: bash

	SGE_ROOT=/opt/sge6/
	SGE_EXECD_PORT=63232
	SGE_QMASTER_PORT=63231

3. Create SQL Tables and Load Static Files
__________________________________________

Once you've configured Cosmos, setting up the SQL database tables is easy.  The web interface is a
:term:`Django` app, which requires you to run the
collectstatic command.  This moves all the necessary image, css, and javascript files to ~/.cosmos/static/ directory.  Run
these two commands after you've configured the database to your liking

.. code-block:: bash

   $ cosmos syncdb
   $ cosmos collectstatic

If you ever switch to a different database in your :file:`~/.cosmos/config.ini`, be sure to run `cosmos syncdb`
to recreate your tables.