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
