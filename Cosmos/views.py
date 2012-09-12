from django.shortcuts import render_to_response
from django.template import RequestContext
import os
import subprocess
from Workflow.models import Workflow

def SGE(request):
    p = subprocess.Popen("qhost", shell=True, stdout=subprocess.PIPE)
    qhost = p.communicate()[0]
    p = subprocess.Popen("qstat", shell=True, stdout=subprocess.PIPE)
    qstat = p.communicate()[0]

    return render_to_response('Cosmos/SGE/index.html', { 'request':request,'qhost': qhost, 'qstat':qstat }, context_instance=RequestContext(request))

def index(request):
    workflows = Workflow.objects.all()
    return render_to_response('index.html', { 'request':request, 'workflows':workflows }, context_instance=RequestContext(request))
