## Install
This probably won't work yet

Core Requirements:
- mysql
- most be run on a DRMAA enabled cluster (GridEngine, LSF, Condor, etc.)

#instructions:
	git clone this
	cd Cosmos
	virtualenv venv
	source venv/bin/activate
	pip install -r pip_requirements.txt

## Example Workflows

Here are some working examples.  see
* [myworkflows/testflow.py](Cosmos/blob/master/my_workflows/testflow.py)
* [myworkflows/call_varaints/cv.py](Cosmos/blob/master/my_workflows/call_variants/cv.py)


## Tutorial

coming soon.

## command.py
command.py is a script that allows you to do things like start the builtin django webserver
or terminate a running workflow.  It is in the root directory of Cosmos.

	$ python command.py -h
	usage: command.py [-h] {terminate,runweb} ...

	positional arguments:
	  {terminate,runweb}
	    terminate
	    runweb

	optional arguments:
	  -h, --help          show this help message and exit


`./manage runserver 0.0.0.0:8000`
