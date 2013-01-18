from django.conf import settings
from cosmos import session

def contproc(request):
    return {
        'cosmos_settings' : session.config,
    }