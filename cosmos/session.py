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
settings = importlib.import_module(os.environ['COSMOS_SETTINGS_MODULE'])

###Setup DJANGO
path = os.path.join(settings.home_path,'cosmos')
if path not in sys.path:
    sys.path.append(path)

os.environ['DJANGO_SETTINGS_MODULE'] = 'cosmos.Cosmos.django_settings'

#DRMAA
import drmaa
drmaa_enabled = True
try:
    drmaa_session = drmaa.Session()
    drmaa_session.initialize()
except Exception as e:
    print "WARNING! WARNING! Could not enable drmaa.  Proceeding without drmaa enabled."
    #print e
    pass
    