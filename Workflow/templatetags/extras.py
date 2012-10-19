from django import template
from django.core.urlresolvers import reverse
import time
from django.core.validators import ValidationError
import re
from JobManager.models import JobAttempt
import sys 

register = template.Library()

@register.filter
def aslist(o):
    return [o]


@register.simple_tag
def get_sjob_stat(batch,field,statistic,pipe=None):
    r = batch.get_sjob_stat(field,statistic)
    return getattr(sys.modules[__name__],pipe)(r) if pipe else r

@register.simple_tag
def get_node_stat(batch,field,statistic,pipe=None):
    r =  batch.get_node_stat(field,statistic)
    return getattr(sys.modules[__name__],pipe)(r) if pipe else r

@register.simple_tag
def convert2int(x):
    if x:
        return int(x)
    else: return x

@register.filter
def underscore2space(s):
    return re.sub('_',' ',s)

@register.filter
def key2val(d, key_name):
    return d[key_name]

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

@register.simple_tag
def format_resource_usage(field_name,val,help_txt):
    if re.search(r"\(Kb\)",help_txt):
        if val == 0: return '0'
        return "{0} ({1})".format(val,format_memory_kb(val))
    elif re.search(r"time",field_name):
        return "{1}".format(val,format_time(val))
    elif field_name=='percent_cpu':
        return "{0}%".format(val)
    elif type(val) in [int,long]:
        return intWithCommas(val)
    return str(val)

def intWithCommas(x):
    if type(x) not in [type(0), type(0L)]:
        raise TypeError("Parameter must be an integer.")
    if x < 0:
        return '-' + intWithCommas(-x)
    result = ''
    while x >= 1000:
        x, r = divmod(x, 1000)
        result = ",%03d%s" % (r, result)
    return "%d%s" % (x, result)
            

@register.filter
def format_memory_kb(kb):
    """converts kb to human readible"""
    if kb is None: return '-'
    mb = kb/1024.0
    gb = mb/1024.0
    if gb > 1:
        return "%s GB" % round(gb,1)
    else:
        return "%s MB" % round(mb,1)
@register.filter
def format_memory_mb(mb):
    """converts mb to human readible"""
    return format_memory_kb(mb*1024.0)

@register.filter
def format_time(seconds):
    if seconds == None: return ''
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)
    if d:
        return "%dd:%02d:%02d:%02d" % (d, h, m, s)
    else:
        return "%d:%02d:%02d" % (h, m, s)
        