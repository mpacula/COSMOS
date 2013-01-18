"""
A Cosmos session.  Must be the first import of any cosmos script.
"""
import os,sys
from cosmos.Cosmos.helpers import confirm
import shutil
from ConfigParser import SafeConfigParser
import shutil

#######################
# Configuration
#######################

user_home_path = os.path.expanduser('~')
cosmos_path = os.path.join(user_home_path,'.cosmos/')
config_path = os.path.join(cosmos_path,'config.ini')
cosmos_library_path = os.path.dirname(os.path.realpath(__file__))
default_config_path = os.path.join(cosmos_library_path,'default_config.ini')

if not os.path.exists(config_path):
    if confirm('No configuration file exists, would you like to create a default one in {0}?'.format(config_path),default=True):
        if not os.path.exists(os.path.dirname(config_path)):
            os.mkdir(os.path.dirname(config_path))
        shutil.copyfile(default_config_path,config_path)
    else:
        sys.exit(1)

cp = SafeConfigParser()
cp.read(config_path)
class settings(object):pass
settings.cosmos_library_path = cosmos_library_path
settings.cosmos_path = cosmos_path
settings.config_path = config_path
settings.user_home_path = user_home_path
for s in cp.sections(): #collapse all sections except Database
    if s!= 'Database':
        for k,v in cp.items(s):
            setattr(settings,k,v)


#######################
# DJANGO
#######################

if 'DJANGO_SETTINGS_MODULE' not in os.environ:
    os.environ['DJANGO_SETTINGS_MODULE'] = 'cosmos.django_settings' #default location for settings
if cosmos_library_path not in sys.path: sys.path.append(cosmos_library_path)

settings.DATABASE = {}
for k,v in cp.items('Database'): settings.DATABASE[k.upper()] = v # Necessary because ConfigParser lowercases keys


#######################
# DRMAA
#######################

if settings.drm == 'LSF':
    os.environ['DRMAA_LIBRARY_PATH'] = settings.drmaa_library_path
    os.environ['DRMAA_LIBRARY_PATH'] = os.path.join(cosmos_library_path,'lsf_drmaa.conf')

import drmaa
drmaa_enabled = False
try:
    drmaa_session = drmaa.Session()
    drmaa_session.initialize()
    drmaa_enabled = True
except Exception as e:
    print e
    print "ERROR! Could not enable drmaa.  Proceeding without drmaa enabled."