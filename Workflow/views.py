from django.shortcuts import render_to_response
from django.template import RequestContext
from models import Workflow, Batch, Node, NodeTag
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from Cosmos.helpers import groupby
from models import status_choices
import re
from models import Q
from django.utils.datastructures import SortedDict

def _get_batches_dict(workflow):
    #batches_dict = Batch.objects.get(workflow=workflow).values('successful',')
    pass

def index(request):
    workflows = Workflow.objects.all()
    return render_to_response('Workflow/index.html', { 'request':request,'workflows': workflows }, context_instance=RequestContext(request))

def view(request,pid):
    workflow = Workflow.objects.get(pk=pid)
    batches_ordered = Batch.objects.filter(workflow=workflow).exclude(order_in_workflow = None).order_by('order_in_workflow')
    return render_to_response('Workflow/view.html', { 'request':request,'workflow': workflow, 'batches_ordered':batches_ordered }, context_instance=RequestContext(request))

def batch_view(request,pid):
    batch = Batch.objects.get(pk=pid)

    #filtering
    #generate possible filter choices
    node_tags = NodeTag.objects.filter(node__batch=batch).values('key','value')
    filter_choices = SortedDict({ 'f_status': [ x[0] for x in status_choices ] }) #init with status filter
    for key,nts in groupby(node_tags,lambda x: x['key']):
        filter_choices[key] = set ([ nt['value'] for nt in nts ]) #add each node_tag.key and all the unique node_tag.values
        
    #filter!
    all_filters = {}
    filter_url=''
    if 'f_status' in request.GET: #user wanted a filter
        all_filters = dict([ (k,request.GET[k]) for k in filter_choices.keys()])
        tag_filters = all_filters.copy()
        for k,v in tag_filters.items():
            if v=='' or v==None or k =='f_status': del tag_filters[k] #filter tag_filters
        nodes_list = batch.get_tagged_nodes(**tag_filters) 
        if request.GET['f_status']: #might be none or ''
            nodes_list = nodes_list.filter(status=request.GET['f_status'])
        
        filter_url = '&'+'&'.join([ '{0}={1}'.format(k,v) for k,v in tag_filters.items() ]) #url to retain this filter
    else:
        nodes_list = Node.objects.filter(batch=batch)


    #pagination
    page_size = 10
    paginator = Paginator(nodes_list, page_size) # Show 25 contacts per page
    page = request.GET.get('page')
    if page is None: page = 1
    try:
        nodes = paginator.page(page)
    except PageNotAnInteger:
        # If page is not an integer, deliver first page.
        nodes = paginator.page(1)
    except EmptyPage:
        # If page is out of range (e.g. 9999), deliver last page of results.
        nodes = paginator.page(paginator.num_pages)
    page_slice = "{0}:{1}".format(page,int(page)+9)
    
        
    return render_to_response('Workflow/Batch/view.html', { 'request':request, 'current_filters':all_filters,'filter_url':filter_url ,'filter_choices':filter_choices, 'batch': batch,'page_size':page_size,'paged_nodes':nodes, 'page_slice':page_slice }, context_instance=RequestContext(request))

def node_view(request,pid):
    node = Node.objects.get(pk=pid)
    jobAttempts_list = node._jobAttempts.all()
    return render_to_response('Workflow/Node/view.html', { 'request':request,'node': node, 'jobAttempts_list':jobAttempts_list }, context_instance=RequestContext(request))

def view_log(request,pid):
    workflow = Workflow.objects.get(pk=pid)
    return render_to_response('Workflow/view_log.html', { 'request':request,'workflow': workflow }, context_instance=RequestContext(request))

#def workflow(request, workflow_id):
#    workflow = Workflow.objects.get(pk=workflow_id)
#    return render_to_response('Workflow/workflow.html', { 'workflow':workflow}, context_instance=RequestContext(request))

