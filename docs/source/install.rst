.. _install:

Install
=======

Requirements
_______________________________________

* Cosmos requires python2.6 or python2.7.

* A Linux environment.  Windows is not supported.

* Some python libraries won't be able to install unless their dependent software is already
  installed on the system.  For example, pygraphviz requires graphviz-dev and
  python-mysql require python-dev libmysqlclient-dev.  If pip install is failing, try running:

.. code-block:: bash

    sudo apt-get update -y
    sudo apt-get install python-dev libmysqlclient-dev graphviz graphviz-dev


Install Method
_______________

Install Cosmos in a virtual environment using
`virtualenvwrapper <http://www.doughellmann.com/projects/virtualenvwrapper/>`_.
This will make sure all python libraries and files related to Cosmos are installed to a sandboxed location in
:file:`$HOME/.virtualenvs/cosmos`.

.. code-block:: bash

    pip install virtualenvwrapper --user
    source $HOME/.local/bin/virtualenvwrapper.sh
    echo "\nsource $HOME/.local/bin/virtualenvwrapper.sh" >> ~/.bash_aliases
    echo "PATH=$HOME/.local/bin:$PATH" >> ~/.bash_aliases

    mkvirtualenv cosmos --no-site-packages
    cd /dir/to/install/Cosmos/to
    pip install distribute --upgrade
    git clone git@github.com:egafni/Cosmos.git --depth=1
    cd Cosmos
    pip install .


Cosmos will be installed to its own python virtual environment, which you can activate by executing the following
`virtualenvwrapper <http://www.doughellmann.com/projects/virtualenvwrapper/>`_ command:

.. code-block:: bash

    $ workon cosmos

Make sure you execute :command:`workon cosmos` anytime you want to interact with Cosmos, or run a script
that uses Cosmos.  Deactivate the virtual environment by executing:

.. code-block:: bash

    $ deactivate
