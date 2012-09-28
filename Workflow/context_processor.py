from django.conf import settings
from cosmos_session import cosmos_settings
 
def contproc(request):
    return {
        'cosmos_settings' : cosmos_settings,
    }