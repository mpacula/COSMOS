"""
Default extra context processor.  Allows cosmos settings to be passed to every Django template.
"""
from cosmos.config import settings

def contproc(request):
    return {
        'cosmos_settings' : settings
    }