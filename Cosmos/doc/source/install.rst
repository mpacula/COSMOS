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
   
   
Other than that, DRMAA must be installed on the system

Optionally, if you want the graphic capabilities, install R and the package ggplot2:

.. code-block:: bash
   sudo apt-get install r # or whatever works on your OS
   sudo R
   > install.packages("ggplot2")