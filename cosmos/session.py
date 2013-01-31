"""
A Cosmos session.  Must be the first import of any cosmos script.
"""
import os,sys
from cosmos.config import settings

#######################
# DJANGO
#######################

#configure django settings
from cosmos import django_settings
from django.conf import settings as django_conf_settings, global_settings
django_conf_settings.configure(
    TEMPLATE_CONTEXT_PROCESSORS=global_settings.TEMPLATE_CONTEXT_PROCESSORS + ('cosmos.utils.context_processor.contproc',),
    **django_settings.__dict__)
#custom template context processor for web interface

#######################
# DRMAA
#######################
os.environ['DRMAA_LIBRARY_PATH'] = settings['drmaa_library_path']
if settings['DRM'] == 'LSF':
    os.environ['LSF_DRMAA_CONF'] = os.path.join(settings['cosmos_library_path'],'lsf_drmaa.conf')

import drmaa
drmaa_enabled = False
try:
    drmaa_session = drmaa.Session()
    drmaa_session.initialize()
    drmaa_enabled = True
except Exception as e:
    print e
    print "ERROR! Could not enable drmaa.  Proceeding without drmaa enabled."