Introduction
============

Welcome!  Cosmos is a workflow management system for Python.  It allows you to quickly program complex workflows that take
advantage of a compute cluster, and provides a web interface to monitor, debug, and analyze your jobs.

Python Library
______________

* Submit stages of jobs to a cluster in parallel (support for :term:`DRMAA` enabled :term:`DRMS` such as SGE, LSF, PBS/Torque, and Condor)
* Wait for stages to finish before proceeding to the next step
* Keep track of job outputs
* Written in python which is easy to learn, powerful, and popular.  A programmer with limited experience can write begin writing Cosmos workflows right away.

Web Interface
_____________

* Cosmos is built using the most popular Python web framework, :term:`Django`, which provides many features like its :term:`ORM`, many Django plug-in apps, and of course, the Cosmos web-interface.
* All your job information is stored into a SQL database (support for PostgreSQL, MySQL, SQLite3 and Oracle).

Design
______

* Cosmos functionality is always aimed to give you resources or make difficult workflow programming tasks easier.
* We try to make sure you can use any command line tool, regardless of its weird input or output requirements.
* You don't write your workflow as some simplified JSON description.  You can use all the logic available in Python to to describe your workflows and its methods of parallelization.
