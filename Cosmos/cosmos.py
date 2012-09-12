import os, sys
from helpers import check_and_create_output_dir, get_logger

#Cosmos Settings
home_path = '/home2/erik/workspace/Cosmos'
default_root_output_dir = '/mnt'

###SGE
os.environ['DRMAA_LIBRARY_PATH'] = '/opt/sge6/lib/linux-x64/libdrmaa.so'
os.environ['SGE_ROOT'] = '/opt/sge6'
os.environ['SGE_EXECD_PORT'] = '63232'
os.environ['SGE_QMASTER_PORT'] = '63231'

###Setup DJANGO
path = home_path
if path not in sys.path:
    sys.path.append(path)

os.environ['DJANGO_SETTINGS_MODULE'] = 'Cosmos.settings'

from django.conf import settings as django_settings

#Validation
#check_and_create_output_dir(default_root_output_dir)

#DRMAA
import drmaa
drmaa_session = drmaa.Session()
drmaa_session.initialize()


#Logger
log_dir = os.path.join(home_path,'log')
check_and_create_output_dir(log_dir)
log = get_logger('cosmos_main', os.path.join(log_dir,'main.log'))
log.info('Starting new cosmos session')