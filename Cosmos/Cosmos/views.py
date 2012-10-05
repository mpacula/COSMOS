from django.shortcuts import render_to_response
from django.template import RequestContext
import os
import subprocess
from Workflow.models import Workflow

def __exec(cmd):
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    c = p.communicate()
    #return 'stdout:\n' +c[0]+'\nstderr:\n'+c[1]
    return c[0]+c[1]

def SGE(request):
    qhost = __exec("qhost")
    qstat = __exec("qstat")

    return render_to_response('Cosmos/SGE/index.html', { 'request':request,'qhost': qhost, 'qstat':qstat }, context_instance=RequestContext(request))

def LSF(request):
    bjobs = __exec("bjobs")
    bqueues = __exec("bqueues")
    lsload = __exec("lsload -l")
    lshosts = __exec("lshosts")
    bhosts = __exec("bhosts")
    bhosts_l = __exec("bhosts -l")

    return render_to_response('Cosmos/LSF/index.html', { 'request':request,'bjobs':bjobs,'bqueues':bqueues,'lsload':lsload,'lshosts':lshosts,'bhosts':bhosts,'bhosts_l':bhosts_l, }, context_instance=RequestContext(request))



def index(request):
    workflows = Workflow.objects.all()
    return render_to_response('index.html', { 'request':request, 'workflows':workflows }, context_instance=RequestContext(request))
