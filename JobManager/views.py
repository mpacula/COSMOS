from django.shortcuts import render_to_response
from django.template import RequestContext
from models import JobAttempt
import math
from django.views.decorators.cache import never_cache

@never_cache
def jobAttempt(request,jobid):
    jobAttempt = JobAttempt.objects.get(pk=jobid)
    return render_to_response('JobManager/JobAttempt/view.html', { 'request':request,'jobAttempt': jobAttempt }, context_instance=RequestContext(request))

@never_cache
def jobAttempt_output(request,jobid,output_name):
    jobAttempt = JobAttempt.objects.get(pk=jobid)
    output_path = jobAttempt.node.output_paths[output_name]
    output = file(output_path,'rb').read(int(math.pow(2,10)*100)) #read at most 100kb
    return render_to_response('JobManager/JobAttempt/output.html', { 'request':request,'output_path':output_path,'output_name':output_name,'output': output, 'jobAttempt':jobAttempt }, context_instance=RequestContext(request))
