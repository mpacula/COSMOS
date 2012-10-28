from django.shortcuts import render_to_response,render
from django.template import RequestContext
from models import Workflow, Batch, Node, NodeTag
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from Cosmos.helpers import groupby
from models import status_choices
from django.utils.datastructures import SortedDict
from django.http import HttpResponse
import os
from django.views.decorators.cache import never_cache
from django.utils.safestring import mark_safe
from django.template import Context, Template

@never_cache
def _get_batches_dict(workflow):
    #batches_dict = Batch.objects.get(workflow=workflow).values('successful',')
    pass

@never_cache
def index(request):
    workflows = Workflow.objects.all()
    return render_to_response('Workflow/index.html', { 'request':request,'workflows': workflows }, context_instance=RequestContext(request))

@never_cache
def view(request,pid):
    workflow = Workflow.objects.get(pk=pid)
    batches_ordered = Batch.objects.filter(workflow=workflow).order_by('order_in_workflow')
    return render_to_response('Workflow/view.html', { 'request':request,'workflow': workflow, 'batches_ordered':batches_ordered }, context_instance=RequestContext(request))

@never_cache
def workflow_batch_table(request,pid):
    """Summary table of a workflow's batches"""
    workflow = Workflow.objects.get(pk=pid)
    batches = Batch.objects.filter(workflow=workflow).order_by('order_in_workflow')
    return render_to_response('Workflow/Batch/table.html', { 'request':request, 'batches': batches }, context_instance=RequestContext(request))

@never_cache
def batch_table(request,pid):
    """Summary table of a batch"""
    batches = [Batch.objects.get(pk=pid)]
    return render_to_response('Workflow/Batch/table.html', { 'request':request, 'batches': batches }, context_instance=RequestContext(request))

def __get_filter_choices(batch):
    """
    :returns: { 'key' : [possible values] } for tags for a batch
    """
    #generate possible filter choices
    nodetags = NodeTag.objects.filter(node__batch=batch).values('key','value')
    filter_choices = SortedDict({ 'f_status': [ x[0] for x in status_choices ] }) #init with status filter
    for key,nts in groupby(nodetags,lambda x: x['key']):
        filter_choices[key] = sorted(set([ nt['value'] for nt in nts ])) #add each node_tag.key and all the unique node_tag.values
    return filter_choices

def __get_context_for_batch_node_table(request,pid):
    """Summary table of nodes.
    :param pid: the pid of the batch
    """
    batch = Batch.objects.get(pk=pid)
    #filtering
    filter_choices = __get_filter_choices(batch)
    #filter!
    all_filters = {}
    filter_url=''
    if 'filter' in request.GET: #user wanted a filter
        all_filters = dict([ (k,request.GET[k]) for k in filter_choices.keys()])
        tag_filters = all_filters.copy()
        for k,v in tag_filters.items():
            if v=='' or v==None or k =='f_status': del tag_filters[k] #filter tag_filters
        nodes_list = batch.get_nodes_by(tag_filters) 
        fs = request.GET.get('f_status')
        if fs != None and fs != '': #might be none or ''
            nodes_list = nodes_list.filter(status=request.GET['f_status'])
            pass
        
        filter_url = 'filter=True&'+'&'.join([ '{0}={1}'.format(k,v) for k,v in all_filters.items() ]) #url to retain this filter
        mark_safe(filter_url)
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
    
    
    return { 'request':request,'batch': batch,'page_size':page_size,'paged_nodes':nodes, 'page_slice':page_slice, 'current_filters':all_filters, 'filter_as_url':filter_url }

@never_cache
def batch_node_table(request,pid):
    context = __get_context_for_batch_node_table(request,pid)
    return render(request,'Workflow/Node/table.html', context)

@never_cache
def batch_view(request,pid):
    batch = Batch.objects.get(pk=pid)
    filter_choices = __get_filter_choices(batch)
    node_table_context = __get_context_for_batch_node_table(request,pid)
    this_context = {'request':request, 'batch': batch, 'filter_choices':filter_choices}
    for k,v in node_table_context.items(): this_context[k] = v 
    
    return render_to_response('Workflow/Batch/view.html', this_context, context_instance=RequestContext(request))

@never_cache
def node_view(request,pid):
    node = Node.objects.get(pk=pid)
    jobAttempts_list = node._jobAttempts.all()
    return render_to_response('Workflow/Node/view.html', { 'request':request,'node': node, 'jobAttempts_list':jobAttempts_list }, context_instance=RequestContext(request))

@never_cache
def view_log(request,pid):
    workflow = Workflow.objects.get(pk=pid)
    return render_to_response('Workflow/view_log.html', { 'request':request,'workflow': workflow }, context_instance=RequestContext(request))

@never_cache
def analysis(request,pid):
    #from rpy2.robjects import r as R
    from django.conf import settings as django_settings
    from cosmos.session import settings as cosmos_settings
    
    wf = Workflow.objects.get(pk=pid)
    
    resultsDir = 'Workflow/plots'
    resultsFile = "{0}.png".format(wf.id)
    resultsFile_path = os.path.join(django_settings.MEDIA_ROOT,resultsDir,resultsFile) 
    plot_url = os.path.join(django_settings.MEDIA_URL,resultsDir,resultsFile)
    plot_path = os.path.join(django_settings.MEDIA_ROOT,resultsDir,resultsFile)
    
    workflow = Workflow.objects.get(pk=pid)
    ru_path = os.path.join(django_settings.MEDIA_ROOT,resultsDir,'resource_usage.csv')
    ru_url = os.path.join(django_settings.MEDIA_URL,resultsDir,'resource_usage.csv')
    plot_rscript_path = os.path.join(cosmos_settings.home_path,'Cosmos/profile/plot.R')
    workflow.save_resource_usage_as_csv(ru_path)
    cmd = 'Rscript {0} {1} {2}'.format(plot_rscript_path,ru_path, plot_path)
    os.system(cmd)
    return render_to_response('Workflow/analysis.html', { 'request':request,'plot_url':plot_url,'resource_usage_url':ru_url}, context_instance=RequestContext(request))



