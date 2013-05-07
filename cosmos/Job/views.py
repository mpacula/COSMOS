from django.shortcuts import render_to_response
from django.template import RequestContext
from models import JobAttempt
import math
from django.views.decorators.cache import never_cache
import os

@never_cache
def jobAttempt(request,jobid):
    jobAttempt = JobAttempt.objects.get(pk=jobid)
    return render_to_response('JobManager/JobAttempt/view.html', { 'request':request,'jobAttempt': jobAttempt }, context_instance=RequestContext(request))

@never_cache
def jobAttempt_profile_output(request,jobid):
    jobAttempt = JobAttempt.objects.get(pk=jobid)
    output_path = jobAttempt.profile_output_path
    if os.path.exist(output_path):
        output = file(output_path,'rb').read(int(math.pow(2,10)*100)) #read at most 100kb
    else:
        output = 'IOError.  File probably does not exist'
    return render_to_response('Workflow/TaskFile/view.html', { 'request': request,'output_path': output_path, 'output_name':'profile output','output': output, 'jobAttempt': jobAttempt }, context_instance=RequestContext(request))
