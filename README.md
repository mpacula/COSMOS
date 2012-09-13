# Install

Core Requirements:
- mysql
- most be run on a DRMAA enabled cluster (GridEngine, LSF, Condor, etc.)

#### install instructions:
	git clone this
	cd Cosmos
	virtualenv venv
	source venv/bin/activate
	pip install -r pip_requirements.txt

#### configuration file:
* edit `cosmos_settings.py`
* set:

	home_path = '/path/to/Cosmos'
	default_root_output_dir = '/mnt/output_dir'

* set any SGE or LSF specific environment variables, if necessary.  For example:

`os.environ['DRMAA_LIBRARY_PATH'] = '/opt/sge6/lib/linux-x64/libdrmaa.so'`


# Example Workflows

Here are some working examples.  see
* [myworkflows/testflow.py](Cosmos/blob/master/my_workflows/testflow.py)
* [myworkflows/call_varaints/cv.py](Cosmos/blob/master/my_workflows/call_variants/cv.py)


# Tutorial

See the example workflows to get started.  Sphinx documentation will be added in the near future.

# Command Line Tool
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


Start the webserver:
`python command.py runweb`

Terminate a running workflow:
`python command.py terminate -n Workflow_Name` or `python command.py terminate -i Workflow_ID#`
