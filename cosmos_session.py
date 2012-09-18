import cosmos_settings

import os,sys

###Setup DJANGO
path = cosmos_settings.home_path
if path not in sys.path:
    sys.path.append(path)

os.environ['DJANGO_SETTINGS_MODULE'] = 'Cosmos.settings'

from django.conf import settings as django_settings

#DRMAA
import drmaa
drmaa_session = drmaa.Session()
drmaa_session.initialize()