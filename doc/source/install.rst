Installation
============

Download Cosmos and install requirements
________________________________________

The following will:

1. Download Cosmos
2. Enable virtualenv
3. Install required python libraries

.. code-block:: bash

   git clone github@git:path/to/Cosmos
   cd Cosmos
   virtualenv --no-site-packages venv
   source venv/bin/activate
   pip install -r pip_requirements.txt
   
   
The only other requirement is that :term:`DRMAA` is installed on the system.

Optionally, if you want the graphing capabilities, R and R package ggplot2 are required:

.. code-block:: bash

   sudo apt-get install r # or whatever works on your OS
   sudo R
   > install.packages("ggplot2")