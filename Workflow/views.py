from django.shortcuts import render_to_response
from django.template import RequestContext
from models import Workflow, Batch, Node
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger

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
    nodes_list = Node.objects.filter(batch=batch)
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
    return render_to_response('Workflow/Batch/view.html', { 'request':request,'batch': batch,'page_size':page_size,'paged_nodes':nodes, 'page_slice':page_slice }, context_instance=RequestContext(request))

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

