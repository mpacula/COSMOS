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

The only other requirement is that :term:`DRMAA` is installed on the system.
If you use multiple python virtual environments, we highly recommend
using .


Quick Install
________________________________________

The following will:

1. Download Cosmos
2. Install Cosmos and required python libraries

.. code-block:: bash

   cd /dir/to/install/Cosmos/to
   git clone git@github.com:ComputationalBiomedicine/Cosmos.git
   pip install Cosmos

Better Install
________________________

Install Cosmos in a virtual environment using
`virtualenvwrapper <http://www.doughellmann.com/projects/virtualenvwrapper/>`_.
This will make sure all python libraries and files related to Cosmos are installed to
$home/.virtualenvs/cosmos.

.. code-block:: bash

    pip install virtualenvwrapper --user
    source $HOME/.local/bin/virtualenvwrapper.sh
    echo "source $HOME/.local/bin/virtualenvwrapper.sh" >> ~/bash.rc
    mkvirtualenv cosmos
    pip install distribute --upgrade
    git clone git@github.com:ComputationalBiomedicine/Cosmos.git
    cd Cosmos
    pip install ./


Now cosmos is installed to its own python virtual environment, which you can activate by typing
:command:`workon cosmos`.  Deactivate the virtual environment by typing :command:`deactivate`


Experimental Features
_________________________

Optionally, if you want the experimental graphing capabilities to automatically summarize
computational resource usage, R and the R package ggplot2 are required.

.. code-block:: bash

   sudo apt-get install r graphviz-dev # or whatever works on your OS
   sudo R
   > install.packages("ggplot2")

