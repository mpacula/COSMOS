Configuration
=============

1. Setup Shell Environment
__________________________

These are environment variables that either drmaa, cosmos, or django require.  Often, its nice to put this
at the end of your ``~/.bashrc`` if you'd like them to be loaded everytime you login.

.. code-block:: bash

   export LIB_DRMAA_PATH=/usr/lib/libdrmaa.so
   export COSMOS_SETTINGS_MODULE=config.default
   export COSMOS_HOME_PATH=/path/to/workspace/Cosmos
   export PYTHONPATH=/path/to/Cosmos:$PYTHONPATH
   export PATH=/path/to/Cosmos/bin:$PATH
   export DJANGO_SETTINGS_MODULE=Cosmos.settings

``COSMOS_SETTING_MODULE`` is optional.  By default, cosmos will look for its configuration in ``config/default.py``,
but if you set ``COSMOS_SETTING_MODULE=config.development`` it will load ``config/development.py`` instead.

SGE specific environment variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
None at the moment

LSF specific environment variables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Feel free to use our included lsf_drmaa.conf lsf_drmaa1.04:

.. code-block:: bash

   export LSF_DRMAA_CONF=$COSMOS_HOME_PATH/config/lsf_drmaa.conf
   

   
2. Edit Configuration File
__________________________

Edit ``config/default.py``, and configure it to your liking.  There are only a few variables to set

Note: It is highly recommended to *not* use an SQL Lite database if the database is stored
on a network shared drive


3. Create SQL Tables
____________________

Once you've configured Cosmos, run:

.. code-block:: bash

   $ cosmos adm syncdb
  
To setup the mysql database