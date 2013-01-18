.. _install:

Install
=======

Download Cosmos and install requirements
________________________________________

The following will:

1. Download Cosmos
3. Install required python libraries

.. code-block:: bash

   pip install github@git:path/to/Cosmos
  
.. note:: Many python libraries won't be able to install unless their dependent software is already installed on the system.  For example, pygraphviz requires graphviz-dev and python-mysql require python-dev libmysqlclient-dev.
   
The only other requirement is that :term:`DRMAA` is installed on the system.  If you use multiple python virtual environments, we highly recommend
using `virtualenvwrapper`<http://www.doughellmann.com/projects/virtualenvwrapper/>.

Optionally, if you want the graphing capabilities, R and the R package ggplot2 are required:

.. code-block:: bash

   sudo apt-get install r graphviz-dev # or whatever works on your OS
   sudo R
   > install.packages("ggplot2")