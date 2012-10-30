"""
A Cosmos session.  Sets up environment variables, Django, SQL and DRMAA
"""
import os,sys
from django.utils import importlib

if 'COSMOS_SETTINGS_MODULE' not in os.environ:
    os.environ['COSMOS_SETTINGS_MODULE'] = 'config.default' #default location for settings
if 'COSMOS_HOME_PATH' not in os.environ:
    print >>sys.stderr, 'please set the environment variable COSMOS_HOME_PATH'
    sys.exit(1)

#parts = os.environ['COSMOS_SETTINGS_MODULE'].split('.')
#package=parts[0]
#name=parts[1]
#settings = getattr(__import__(package, fromlist=[name]), name)
try:
    settings = importlib.import_module(os.environ['COSMOS_SETTINGS_MODULE'])
except ImportError:
    print >> sys.stderr, "ERROR!! The file {} in the COSMOS_SETTINGS_MODULE environment variable could not be imported.".format(os.environ['COSMOS_SETTINGS_MODULE'])
    sys.exit(1)

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
    