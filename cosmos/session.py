"""
A Cosmos session.  Sets up environment variables, Django, SQL and DRMAA
"""
import os,sys
from django.utils import importlib
from cosmos.Cosmos.helpers import confirm
import shutil
from ConfigParser import SafeConfigParser
import shutil

#import Cosmos Settings
if 'DJANGO_SETTINGS_MODULE' not in os.environ:
    os.environ['DJANGO_SETTINGS_MODULE'] = 'cosmos.django_settings' #default location for settings

#######################
# Setup Settings
#######################
config_path = os.path.join(os.path.expanduser('~'),'.cosmos/config.ini')
cosmos_library_path = os.path.dirname(os.path.realpath(__file__))
default_config_filepath = os.path.join(cosmos_library_path,'config.ini')

if not os.path.exists(config_path):
    if confirm('No configuration file exists, would you like to create a default one in {0}?'.format(config_path),default=True):
        if not os.path.exists(os.path.dirname(config_path)):
            os.mkdir(os.path.dirname(config_path))
        shutil.copyfile(default_config_filepath,config_path)
    else:
        sys.exit(1)

cp = SafeConfigParser()
cp.read(config_path)
class settings(object):pass
settings.cosmos_library_path = cosmos_library_path
settings.home_path = '~/.cosmos'
for s in cp.sections(): #collapse all sections except Database
    if s!= 'Database':
        for k,v in cp.items(s):
            setattr(settings,k,v)


#necessary for Django
if cosmos_library_path not in sys.path: sys.path.append(cosmos_library_path)

settings.DATABASE = {}
for k,v in cp.items('Database'): settings.DATABASE[k.upper()] = v # Necessary because ConfigParser lowercases keys


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