from cosmos.config import settings

def contproc(request):
    return {
        'cosmos_settings' : settings
    }