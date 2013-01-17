"""
A Cosmos session.  Sets up environment variables, Django, SQL and DRMAA
"""
import os,sys
from django.utils import importlib
from cosmos.Cosmos.helpers import confirm
import shutil

#import Cosmos Settings
if 'DJANGO_SETTINGS_MODULE' not in os.environ:
    os.environ['DJANGO_SETTINGS_MODULE'] = 'cosmos.Cosmos.django_settings' #default location for settings
if 'COSMOS_SETTINGS_MODULE' not in os.environ:
    os.environ['COSMOS_SETTINGS_MODULE'] = os.path.join(os.path.expanduser('~'),'.cosmos/config.py') #default location for settings



#######################
# Setup Default Config
#######################
config_path = os.environ['COSMOS_SETTINGS_MODULE']
if not os.path.exists(config_path):
    if confirm('No configuration file exists, would you like to create a default one in {0}?'.format(config_path),default=True):
        os.mkdir(os.path.dirname(config_path))
        with open(config_path,'w') as f:
            f.write(
"""
import os
#os.environ['DJANGO_SETTINGS_MODULE'] = 'cosmos.Cosmos.django_settings'
#os.environ['COSMOS_HOME_PATH'] = '/home/erik/workspace/cosmos'

#######################
# Cosmos
#######################

default_root_output_dir = '/tmp/cosmos_out' # The directory to output all files to
DRM = 'GE' #LSF, or GE
tmp_dir = '/tmp'
default_queue = None #Default Queue name to use if a workflow's default_queue is set to None.

########################
# Web interface
########################

# on some systems, file i/o slows down a lot when running a lot of jobs making file_size calculations very slow.
show_stage_file_sizes = False
show_jobAttempt_file_sizes = False
show_task_file_sizes = False

########################
# Database
########################

DATABASE = {
    'ENGINE': 'django.db.backends.sqlite3', # Add 'postgresql_psycopg2', 'postgresql', 'mysql', 'sqlite3' or 'oracle'.
    'NAME': '~/.cosmos/sqlite.db',                      # Or path to database file if using sqlite3.
    'USER': '',                      # Not used with sqlite3.
    'PASSWORD': '',                  # Not used with sqlite3.
    'HOST': '',                      # Set to empty string for localhost. Not used with sqlite3.
    'PORT': '',                      # Set to empty string for default. Not used with sqlite3.
}


### SGE Specific
parallel_environment_name = 'orte' #the name of the SGE parallel environment name.  Use "qconf -spl" to list available names
""")


try:
    settings = importlib.import_module(config_path)
except ImportError:
    print >> sys.stderr, "ERROR!! The config module {} could not be imported.".format(config_path)
    sys.exit(1)

#if 'COSMOS_HOME_PATH' not in os.environ:
#    print >>sys.stderr, 'please set the environment variable COSMOS_HOME_PATH'
#    sys.exit(1)

#parts = os.environ['COSMOS_SETTINGS_MODULE'].split('.')
#package=parts[0]
#name=parts[1]
#settings = getattr(__import__(package, fromlist=[name]), name)

###Setup DJANGO
path = os.path.join(settings.home_path,'cosmos')
if path not in sys.path:
    sys.path.append(path)

#DRMAA
import drmaa
drmaa_enabled = False
try:
    drmaa_enabled = True
    drmaa_session = drmaa.Session()
    drmaa_session.initialize()
    
except Exception as e:
    print e
    print "ERROR! Could not enable drmaa.  Proceeding without drmaa enabled."
    
    pass
    