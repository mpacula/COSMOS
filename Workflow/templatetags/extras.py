from django import template
from django.core.urlresolvers import reverse
import time
from django.core.validators import ValidationError

register = template.Library()

@register.simple_tag
def navactive(request, name):
    if name=='home' and request.path=='/':
        return 'active'
    elif name in request.path.split('/'):
        return "active"
    return ""

@register.simple_tag
def status2csstype(status):
    d = {'failed':'danger',
     'successful':'success',
     'no_attempt':'info',
     'in_progress':'warning'}
    return d[status]

@register.filter
def mult(value, arg):
    "Multiplies the arg and the value"
    return int(value) * int(arg)

@register.filter
def format_time(seconds):
    if seconds == 0:
        return '00:00:00'
    if seconds is None or seconds == 'None' or seconds == '':
        return 'None'
    try:
        return time.strftime('%H:%M:%S', time.gmtime(seconds))
    except TypeError:
        raise ValidationError('Expected a float, got'.format(seconds))