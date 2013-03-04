.. _install:

Install
=======

Requirements
_______________________________________

Many python libraries won't be able to install unless their dependent software is already
installed on the system.  For example, pygraphviz requires graphviz-dev and
python-mysql require python-dev libmysqlclient-dev.  If pip install is failing, try running:
:command:`apt-get update -y`
:command:`apt-get install python-dev libmysqlclient-dev mysql-server graphviz graphviz-dev`

The only other requirement is that :term:`DRMAA` is installed on the system if you want Cosmos to submit
jobs to a :term:`DRMS` like LSF or Grid Engine.

Quick Install
________________________________________


This is generally for advanced users who have worked with python packages before.

.. code-block:: bash

   cd /dir/to/install/Cosmos/to
   git clone git@github.com:ComputationalBiomedicine/Cosmos.git --depth=1
   pip install distribute --upgrade
   cd Cosmos
   pip install .

.. hint::

    You need root access to install python packages to the system directories.  You may have to run pip install with
    'sudo pip install ...', or to install to the user level use 'pip install --user'.  99% of users should just
    use the *Better Install* described next.

Better Install
________________________

Install Cosmos in a virtual environment using
`virtualenvwrapper <http://www.doughellmann.com/projects/virtualenvwrapper/>`_.
This will make sure all python libraries and files related to Cosmos are installed to a sandboxed location in
:file:`$HOME/.virtualenvs/cosmos`.

.. code-block:: bash

    pip install virtualenvwrapper --user
    source $HOME/.local/bin/virtualenvwrapper.sh
    echo "source $HOME/.local/bin/virtualenvwrapper.sh" >> ~/bash.rc

    mkvirtualenv cosmos
    cd /dir/to/install/Cosmos/to
    pip install distribute --upgrade
    git clone git@github.com:ComputationalBiomedicine/Cosmos.git --depth=1
    cd Cosmos
    pip install .


Now cosmos is installed to its own python virtual environment, which you can activate by typing
:command:`workon cosmos`.  Make sure you type `workon cosmos` anytime you want to interact with Cosmos, or run a script
that uses Cosmos.
Deactivate the virtual environment by typing :command:`deactivate`.


Experimental Features
_________________________

Optionally, if you want the experimental graphing capabilities to automatically summarize
computational resource usage, R and the R package ggplot2 are required.

.. code-block:: bash

   sudo apt-get install r graphviz-dev # or whatever works on your OS
   sudo R
   > install.packages("ggplot2")

