from django.db import models, transaction
from django.db.models import Q
from cosmos.JobManager.models import JobAttempt,JobManager
import os,sys,re,signal
from cosmos.Cosmos.helpers import get_drmaa_ns,validate_name,validate_not_null, check_and_create_output_dir, folder_size, get_workflow_logger
from cosmos.Cosmos import helpers
from django.core.exceptions import ValidationError
from picklefield.fields import PickledObjectField
from cosmos import session
from django.utils import timezone
import networkx as nx
import pygraphviz as pgv

status_choices=(
                ('successful','Successful'),
                ('no_attempt','No Attempt'),
                ('in_progress','In Progress'),
                ('failed','Failed')
                )


class Workflow(models.Model):
    """   
    This is the master object.  It contains a list of :class:`Batch` which represent a pool of jobs that have no dependencies on each other
    and can be executed at the same time. 
    """
    name = models.CharField(max_length=250,unique=True)
    output_dir = models.CharField(max_length=250)
    jobManager = models.OneToOneField(JobManager,null=True, related_name='workflow')
    dry_run = models.BooleanField(default=False,help_text="don't execute anything")
    max_reattempts = models.SmallIntegerField(default=3)
    default_queue = models.CharField(max_length=255,default=None,null=True)
    
    created_on = models.DateTimeField(null=True,default=None)
    finished_on = models.DateTimeField(null=True,default=None)
    
    
    def __init__(self, *args, **kwargs):
        kwargs['created_on'] = timezone.now()
        super(Workflow,self).__init__(*args, **kwargs)
        
        validate_name(self.name)
        #Validate unique name
        if Workflow.objects.filter(name=self.name).exclude(pk=self.id).count() >0:
            raise ValidationError('Workflow with name {0} already exists.  Please choose a different one or use .__resume()'.format(self.name))
        #create jobmanager
        if not hasattr(self,'jobManager') or self.jobManager is None:
            self.jobManager = JobManager.objects.create()
            self.jobManager.save()
            
        check_and_create_output_dir(self.output_dir)
        
        self.log, self.log_path = get_workflow_logger(self) 
        
    @property
    def nodes(self):
        """Nodes in this Workflow"""
        return Node.objects.filter(batch__in=self.batch_set.all())
    
    @property
    def node_edges(self):
        """Edges in this Workflow"""
        return NodeEdge.objects.filter(parent__in=self.nodes)
    
    @property
    def wall_time(self):
        """Time between thisworkflowh's creation and finished datetimes.  Note, this is a timedelta instance, not seconds"""
        return self.finished_on - self.created_on if self.finished_on else timezone.now().replace(microsecond=0) - self.created_on
    
    @property
    def total_batch_wall_time(self):
        """
        Sum(batch_wall_times).  Can be different from workflow.wall_time due to workflow stops and resumes.
        """
        return reduce(lambda x,y: x+y, [b.wall_time for b in self.batches ])
    
    @property
    def batches(self):
        """Batches in this Workflow"""
        return self.batch_set.all()
    
    @property
    def file_size(self):
        """Size of the output directory"""
        return folder_size(self.output_dir)
        
    @property
    def log_txt(self):
        """Path to the logfile"""
        return file(self.log_path,'rb').read()
    
    @staticmethod
    def start(name=None, restart=False, dry_run=False, root_output_dir=None, default_queue=None):
        """
        Starts a workflow.  If a workflow with this name already exists, return the workflow.
        
        :param name: (str) A unique name for this workflow. All spaces are converted to underscores. Required.
        :param restart: (bool) Restart the workflow by deleting it and creating a new one. Optional.
        :param dry_run: (bool) Don't actually execute jobs. Optional.
        :param root_output_dir: (bool) Replaces the directory used in settings as the workflow output directory. If None, will use default_root_output_dir in the config file. Optional.
        :param default_queue: (str) Name of the default queue to submit jobs to. Optional.
        """
        
        if name is None:
            raise ValidationError('Name of a workflow cannot be None')
        name = re.sub("\s","_",name)
        
        if root_output_dir is None:
            root_output_dir = session.settings.default_root_output_dir
            
        if restart:
            wf = Workflow.__restart(name=name, root_output_dir=root_output_dir, dry_run=dry_run, default_queue=default_queue)
        elif Workflow.objects.filter(name=name).count() > 0:
            wf = Workflow.__resume(name=name, dry_run=dry_run, default_queue=default_queue)
        else:
            wf = Workflow.__create(name=name, dry_run=dry_run, root_output_dir=root_output_dir, default_queue=default_queue)
        
        #remove stale objects
        wf._delete_stale_objects()
        
        #terminate on ctrl+c
        def ctrl_c(signal,frame):
                wf.terminate()
        try:
            signal.signal(signal.SIGINT, ctrl_c)
        except ValueError: #signal only works in main thread
            pass
        
        return wf
        
    
    @staticmethod
    def __resume(name=None,dry_run=False, default_queue=None):
        """
        Resumes a workflow from the last failed node.
        
        :param name: (str) A unique name for this workflow
        :param dry_run: (bool) Don't actually execute jobs. Optional.
        :param root_output_dir: (bool) Replaces the directory used in settings as the workflow output directory. If None, will use default_root_output_dir in the config file. Optional.
        :param default_queue: (str) Name of the default queue to submit jobs to. Optional.
        """

        if Workflow.objects.filter(name=name).count() == 0:
            raise ValidationError('Workflow {0} does not exist, cannot resume it'.format(name))
        wf = Workflow.objects.get(name=name)
        wf.dry_run=dry_run
        wf.finished_on = None
        wf.default_queue=default_queue
        
        wf.save()
        wf.log.info('Resuming workflow.')
        Batch.objects.filter(workflow=wf).update(order_in_workflow=None)
        
        return wf

    @staticmethod
    def __restart(name=None,root_output_dir=None,dry_run=False,default_queue=None,prompt_confirm=True):
        """
        Restarts a workflow.  Will delete the old workflow and all of its files
        but will retain the old workflow id for convenience
        
        :param name: (name) A unique name for this workflow. All spaces are converted to underscores. 
        :param dry_run: (bool) Don't actually execute jobs. Optional.
        :param root_output_dir: (bool) Replaces the directory used in settings as the workflow output directory. If None, will use default_root_output_dir in the config file. Optional.
        :param default_queue: (str) Name of the default queue to submit jobs to. Optional.
        :param prompt_confirm: (bool) If True, will prompt the user for a confirmation before deleting the workflow.
        """
        wf_id = None
        if Workflow.objects.filter(name=name).count():
            if prompt_confirm:
                if not helpers.confirm("Are you sure you want to restart Workflow '{0}'?  All files will be deleted.".format(name),default=True,timeout=30):
                    print "Exiting."
                    sys.exit(1)
            old_wf = Workflow.objects.get(name=name)
            wf_id = old_wf.id
            old_wf.delete()
        
        new_wf = Workflow.__create(_wf_id=wf_id, name=name, root_output_dir=root_output_dir, dry_run=dry_run, default_queue=default_queue)
        
        return new_wf
                
    @staticmethod
    def __create(name=None,dry_run=False,root_output_dir=None,_wf_id=None,default_queue=None):
        """
        Creates a new workflow
        
        :param name: (str) A unique name for this workflow
        :param dry_run: (bool) Don't actually execute jobs. Optional.
        :param root_output_dir: (bool) Replaces the directory used in settings as the workflow output directory. If None, will use default_root_output_dir in the config file. Optional.
        :param default_queue: (str) Name of the default queue to submit jobs to. Optional.
        """
        if Workflow.objects.filter(id=_wf_id).count(): raise ValidationError('Workflow with this _wf_id already exists')
        check_and_create_output_dir(root_output_dir)
        output_dir = os.path.join(root_output_dir,name)
        
        wf = Workflow.objects.create(id=_wf_id,name=name, output_dir=output_dir, dry_run=dry_run, default_queue=default_queue)
        wf.log.info('Created Workflow {0}.'.format(wf))
        wf.save()
        return wf
            
        
    def add_batch(self, name, hard_reset=False):
        """
        Adds a batch to this workflow.  If a batch with this name (in this Workflow) already exists,
        and it hasn't been added in this session yet, return the existing one.
        
        :parameter name: (str) The name of the batch, must be unique within this Workflow. Required.
        :parameter hard_reset: (bool) Delete any batch with this name including all of its nodes, and return a new one. Optional.
        """
        #TODO name can't be "log" or change log dir to .log
        name = re.sub("\s","_",name)
        self.log.info("Adding batch {0}.".format(name))
        #determine order in workflow
        m = Batch.objects.filter(workflow=self).aggregate(models.Max('order_in_workflow'))['order_in_workflow__max']
        if m is None:
            order_in_workflow = 1
        else:
            order_in_workflow = m+1
        
        batch_exists = Batch.objects.filter(workflow=self,name=name).count()>0
        _old_id = None
        if batch_exists:
            old_batch = Batch.objects.get(workflow=self,name=name)
            _old_id = old_batch.id
            if old_batch.status == 'failed':
                if helpers.confirm('{0} has a status of failed.  Would you like to perform a hard_reset on it before proceeding? Answering no will proceed with the workflow with the batch left unchanged.  Unsuccessful nodes will be re-run.'.format(old_batch),default=True):
                    old_batch.delete()
            if hard_reset:
                if helpers.confirm("Are you sure you want to do a hard reset on {0}?".format(old_batch),default=True,timeout=30):
                    self.log.info("Doing a hard reset on {0}.".format(old_batch))
                    old_batch.delete()
                else:
                    self.log.info("Exiting.")
                    sys.exit(1)
                
        b, created = Batch.objects.get_or_create(workflow=self,name=name,id=_old_id)
        if created:
            self.log.info('Creating {0} from scratch.'.format(b))
        else:
            self.log.info('{0} already exists, loading it from history...'.format(b))
            self.finished_on = None #resuming, so reset this
            
        b.order_in_workflow = order_in_workflow
        b.save()
        return b

    def _delete_stale_objects(self):
        """
        Deletes objects that are stale from the database.  This should only happens when the program exists ungracefully.
        """
        #TODO implement a catch all exception so that this never happens.  i think i can only do this if scripts are not run directly
        for ja in JobAttempt.objects.filter(node_set=None): ja.delete()
        
    
    def terminate(self):
        """
        Terminates this workflow and Exits
        """
        self.log.warning("Terminating this workflow...")
        self.save()
        jobAttempts = self.jobManager.jobAttempts.filter(queue_status='queued')
        jids = [ ja.id for ja in jobAttempts ]
        drmaa_jids = [ ja.drmaa_jobID for ja in jobAttempts ]
        #jobIDs = ', '.join(ids)
        #cmd = 'qdel {0}'.format(jobIDs)
        for jid in drmaa_jids:
            cmd = 'qdel {0}'.format(jid)
            os.system(cmd)
        
        #this basically a bulk node._has_finished and jobattempt.hasFinished
        self.log.info("Marking all terminated JobAttempts as failed.")
        jobAttempts.update(queue_status='completed',finished_on = timezone.now())
        nodes = Node.objects.filter(_jobAttempts__in=jids)

        self.log.info("Marking all terminated Nodes as failed.")
        nodes.update(status = 'failed',finished_on = timezone.now())
        
        self.log.info("Marking all terminated Batches as failed.")
        batches = Batch.objects.filter(pk__in=nodes.values('batch').distinct())
        batches.update(status = 'failed',finished_on = timezone.now())
        
        self.finished()
        
        self.log.info("Exiting.")
        sys.exit(1)
    
    def get_all_tag_keywords_used(self):
        """Returns a set of all the keyword tags used on any node in this workflow"""
        return set([ d['key'] for d in NodeTag.objects.filter(node__in=self.nodes).values('key') ])
    
    def save_resource_usage_as_csv(self,filename):
        """Save resource usage to filename"""
        import csv
        profile_fields = JobAttempt.profile_fields_as_list()
        keys = ['batch'] + list(self.get_all_tag_keywords_used()) + profile_fields
        f = open(filename, 'wb')
        dict_writer = csv.DictWriter(f, keys)
        dict_writer.writer.writerow(keys)
        for batch_resources in self.yield_batch_resource_usage():
            dict_writer.writerows(batch_resources)

    def yield_batch_resource_usage(self):
        """
        :yields: A dict of all resource usage, tags, and the name of the batch of every node
        """
        for batch in self.batches:
            dicts = [ dict(nru) for nru in batch.yield_node_resource_usage() ]
            for d in dicts: d['batch'] = re.sub('_',' ',batch.name)
            yield dicts
    
    @transaction.commit_on_success
    def bulk_save_nodes(self,data):
        """
        Does a bulk insert to speedup adding lots of nodes.  Will filter out any None values in the nodes_and_tags list.
        
        :param data: (list of {Node,dict,bool,parent_nodes)}) ie: [(node1,tags1,node_exists,parents),(node2,tags2,node_exists,parent_nodes),...]
        
        >>> nodes = [(batch.add_node(name='1'pcmd='cmd1',save=False),{},True,[]),(batch.add_node(name='2',pcmd='cmd2',save=False,{},True,[]))]
        >>> batch.bulk_add_nodes(nodes)
        """
        ### Bulk add nodes
        self.log.info("Bulk adding {0} nodes...".format(len(data)))
        filtered_data = filter(lambda x: not x['node_exists'],data)
        if len(filtered_data) == 0:
            return []
        
        #need to manually set IDs because there's no way to get them in the right order for tagging after a bulk create
        m = Node.objects.all().aggregate(models.Max('id'))['id__max']
        id_start =  m + 1 if m else 1
        for i,d in enumerate(filtered_data): d['node'].id = id_start + i
        
        Node.objects.bulk_create(map(lambda d: d['node'], filtered_data))
        #create output directories
        for n in map(lambda d: d['node'], filtered_data):
            os.mkdir(n.output_dir)
            os.mkdir(n.job_output_dir) #this is not in JobManager because JobMaster should be not care about these details
        
        ### Bulk add tags
        nodetags = []
        for d in filtered_data:
            for k,v in d['tags'].items():
                nodetags.append(NodeTag(node=d['node'],key=k,value=v))
        self.log.info("Bulk adding {0} node tags...".format(len(nodetags)))
        NodeTag.objects.bulk_create(nodetags)
        
        ### Bulk add parents
        node_edges = []
        for  d in filtered_data:
            for parent in d['parents']:
                node_edges.append(NodeEdge(parent=parent,child=d['node'],tags=d['tags']))
        self.log.info("Bulk adding {0} NodeEdges...".format(len(node_edges)))
        NodeEdge.objects.bulk_create(node_edges)
        
        return
            
    def delete(self, *args, **kwargs):
        """
        Deletes this workflow.
        """
        self.log.info("Deleting {0}...".format(self))
        for h in list(self.log.handlers):
            h.close()
            self.log.removeHandler(h)
        
#        if kwargs.setdefault('delete_files',False):
#            kwargs.pop('delete_files')
#            self.log.info('Deleting directory {0}'.format(self.output_dir))
        if os.path.exists(self.output_dir):
            os.system('rm -rf {0}'.format(self.output_dir))
            
        self.jobManager.delete()
                
        for b in self.batches: b.delete()
        
        super(Workflow, self).delete(*args, **kwargs)
                


    def _run_node(self,node):
        """
        Creates and submits and JobAttempt.
        
        :param node: the node to submit a JobAttempt for
        """
        
        node.batch.status = 'in_progress'
        node.batch.save()
        node.status = 'in_progress'
        self.log.info('Running {0} from {1}'.format(node,node.batch))
        try:
            node.exec_command = node.pre_command.format(output_dir=node.job_output_dir,outputs = node.outputs)
        except KeyError:
            helpers.formatError(node.pre_command,{'output_dir':node.job_output_dir,'outputs': node.outputs})
                
        #create command.sh that gets executed
        
        jobAttempt = self.jobManager.add_jobAttempt(command=node.exec_command,
                                     drmaa_output_dir=os.path.join(node.output_dir,'drmaa_out/'),
                                     jobName="",
                                     drmaa_native_specification=get_drmaa_ns(DRM=session.settings.DRM,
                                                                             mem_req=node.memory_requirement,
                                                                             cpu_req=node.cpu_requirement,
                                                                             queue=self.default_queue))
        
        node._jobAttempts.add(jobAttempt)
        if self.dry_run:
            self.log.info('Dry Run: skipping submission of job {0}.'.format(jobAttempt))
        else:
            self.jobManager.submit_job(jobAttempt)
            self.log.info('Submitted jobAttempt with drmaa jobid {0}.'.format(jobAttempt.drmaa_jobID))
        node.save()
        self.jobManager.save()
        return jobAttempt

    def run_batch(self,batch):
        """
        Runs any unsuccessful nodes of a batch
        """
        self.log.info('Running batch {0}.'.format(batch))
        
        if batch.successful:
            self.log.info('{0} has already been executed successfully, skip run.'.format(batch))
            return
        for node in batch.nodes:
            if node.successful:
                self.log.info('{0} has already been executed successfully, skip run.'.format(node))
            else:
                self.log.debug('{0} has not been executed successfully yet.'.format(node))
                self._run_node(node)


    def _reattempt_node(self,node,failed_jobAttempt):
        """
        Reattempt running a node.
        
        :param node: (Node) the node to reattempt
        :param failed_jobAttempt: (bool) the previously failed jobAttempt of the node
        :returns: (bool) True if another jobAttempt was submitted, False if the max jobAttempts has already been reached
        """
        numAttempts = node.jobAttempts.count()
        if not node.successful: #ReRun jobAttempt
            if numAttempts < self.max_reattempts:
                self.log.warning("JobAttempt {0} of node {1} failed, on attempt # {2}, so deleting failed output files and retrying".format(failed_jobAttempt, node,numAttempts))
                os.system('rm -rf {0}/*'.format(node.job_output_dir))
                self._run_node(node)
                return True
            else:
                self.log.warning("Node {0} has reached max_reattempts of {0}.  This node has failed".format(self, self.max_reattempts))
                self.status = 'failed'
                self.save()
                return False
    
    def wait(self,batch=None,terminate_on_fail=False):
        """
        Waits for all executing nodes to finish.  Returns an array of the nodes that finished.
        if batch is omitted or set to None, all running nodes will be waited on.
        
        :param batch: (Batch) wait for all of a batch's nodes to finish
        :param terminate_on_fail: (bool) If True, the workflow will self terminate of any of the nodes of this batch fail `max_job_attempts` times
        """
        nodes = []
        if batch is None:
            self.log.info('Waiting on all nodes...')
        else:
            self.log.info('Waiting on batch {0}...'.format(batch))
        
        for jobAttempt in self.jobManager.yield_all_queued_jobs():
            node = jobAttempt.node
            #self.log.info('Finished {0} for {1} of {2}'.format(jobAttempt,node,node.batch))
            nodes.append(node)
            if jobAttempt.successful:
                node._has_finished(jobAttempt)
            else:
                submitted_another_job = self._reattempt_node(node,jobAttempt)
                if not submitted_another_job:
                    node._has_finished(jobAttempt) #job has failed and out of reattempts
                    if terminate_on_fail:
                        self.log.warning("{0} has reached max_reattempts and terminate_on_fail==True so terminating.".format(node))
                        self.terminate()
            if batch and batch.is_done():
                break;
            
                    
        if batch is None: #no waiting on a batch
            self.log.info('All nodes for this wait have completed!')
        else:
            self.log.info('All nodes for the wait on {0} completed!'.format(batch))
        
        return nodes 

    def run_wait(self, batch, terminate_on_fail=True):
        """
        Shortcut to run_batch(); wait(batch=batch,terminate_on_fail=terminate_on_fail);
        """
        self.run_batch(batch=batch)
        return self.wait(batch=batch,terminate_on_fail=terminate_on_fail)

    def finished(self,delete_unused_batches=False):
        """
        Call at the end of every workflow.
        If there any left over jobs that have not been collected,
        It will wait for all of them them
        
        :param delete_unused_batches: (bool) Any batches and their output_dir from previous workflows that weren't loaded since the last create, __resume, or __restart, using add_batch() are deleted.
        
        """
        self._check_and_wait_for_leftover_nodes()
        
        self.log.debug("Cleaning up workflow")
        if delete_unused_batches:
            self.log.info("Deleting unused batches")
            for b in Batch.objects.filter(workflow=self,order_in_workflow=None): b.delete() #these batches weren't used again after a __restart
            
        self.finished_on = timezone.now()
        self.save()
        self.log.info('Finished.')
        
    def _check_and_wait_for_leftover_nodes(self):
        """Checks and waits for any leftover nodes"""
        if self.nodes.filter(status='in_progress').count()>0:
            self.log.warning("There are left over nodes in the queue, waiting for them to finish")
            self.wait()
               
    
    def restart_from_here(self):
        """
        Deletes any batches in the history that haven't been added yet
        """
        if helpers.confirm("Are you sure you want to run restart_from_here() on workflow {0}?  All files will be deleted.".format(self),default=True,timeout=30):
            self.log.info('Restarting Workflow from here.')
            for b in Batch.objects.filter(workflow=self,order_in_workflow=None): b.delete()
    
    def get_nodes_by(self,batch=None,tags={},op="and"):
        """
        Returns the list of nodes that are tagged by the keys and vals in tags dictionary
        
        :param op: (str) either 'and' or 'or' as the logic to filter tags with
        :param tags: (dict) tags to filter for
        :returns: (queryset) a queryset of the filtered nodes
        
        >>> node.get_nodes_by(op='or',tags={'color':'grey','color':'orange'})
        >>> node.get_nodes_by(op='and',tags={'color':'grey','shape':'square'})
        """
        
        if op == 'or':
            raise NotImplemented('sorry')
        
        if batch:
            nodes = batch.nodes
        else:
            nodes = self.nodes
            
        if tags == {}:
            return nodes    
        else:    
            for k,v in tags.items():
                nodes = nodes.filter(nodetag__key=k, nodetag__value=v)
                
            return nodes

    def get_node_by(self,tags={},batch=None,op="and"):
        """
        Returns the list of nodes that are tagged by the keys and vals in tags dictionary.
        
        :raises Exception: if more or less than one node is returned
        
        :param op: (str) Choose either 'and' or 'or' as the logic to filter tags with
        :param tags: (dict) A dictionary of tags you'd like to filter for
        :returns: (queryset) a queryset of the filtered nodes
        
        >>> node.get_node_by(op='or',tags={'color':'grey','color':'orange'})
        >>> node.get_node_by(op='and',tags={'color':'grey','color':'orange'})
        """
    
        nodes = self.get_nodes_by(batch=batch,op=op,tags=tags) #there's just one group of nodes with this tag combination
        n = nodes.count()
        if n>1:
            raise Exception("More than one node with tags {0}".format(tags))
        elif n == 0:
            raise Exception("No nodes with with tags {0}.".format(tags))
        return nodes[0]
    
    def __str__(self):
        return 'Workflow[{0}] {1}'.format(self.id,re.sub('_',' ',self.name))
    
    @models.permalink    
    def url(self):
        return ('workflow_view',[str(self.id)])

class WorkflowDAG():
    def __init__(self,workflow):
        self.workflow = workflow
        
    def createAGraph(self):
        G = pgv.AGraph(strict=False,directed=True)
        G.add_edges_from([(ne.parent,ne.child) for ne in self.workflow.node_edges])
        
        for batch in self.workflow.batches:
            sg = G.add_subgraph(name="cluster_{0}".format(batch.name),label=batch.name,color='lightgrey')
            for n in batch.nodes:
                sg.add_node(n,label=n.id)
            #sg.set_attr()
            
            
    
        return G
    
    def as_img(self):        
        g = self.createAGraph()
        g.layout(prog="dot")
        return g.draw(format="svg")
        
    def __str__(self):
        return self.createAGraph().to_string()
    
        


class Batch(models.Model):
    """
    A group of jobs that can be run independently.  See `Embarassingly Parallel <http://en.wikipedia.org/wiki/Embarrassingly_parallel>`_ .
    
    .. note:: A Batch should not be directly instantiated, use :py:func:`Workflow.models.Workflow.add_batch` to create a new batch.
    """
    name = models.CharField(max_length=200)
    workflow = models.ForeignKey(Workflow)
    order_in_workflow = models.IntegerField(null=True)
    status = models.CharField(max_length=200,choices=status_choices,default='no_attempt') 
    successful = models.BooleanField(default=False)
    created_on = models.DateTimeField(null=True,default=None)
    finished_on = models.DateTimeField(null=True,default=None)
    
    
    def __init__(self,*args,**kwargs):
        kwargs['created_on'] = timezone.now()
        super(Batch,self).__init__(*args,**kwargs)
        
        validate_not_null(self.workflow)
        check_and_create_output_dir(self.output_dir)
        
        validate_name(self.name,'Batch_Name')
        #validate unique name
        if Batch.objects.filter(workflow=self.workflow,name=self.name).exclude(id=self.id).count() > 0:
            raise ValidationError("Batch names must be unique within a given Workflow. The name {0} already exists.".format(self.name))

    @property
    def log(self):
        return self.workflow.log
    
    @property
    def percent_done(self):
        """
        Percent of nodes that have completed
        """
        done = Node.objects.filter(batch=self,successful=True).count()
        total = self.num_nodes
        status = self.status
        if total == 0 or done == 0:
            if status == 'in_progress' or status == 'failed':
                return 1
            return 0
        r = int(100 * float(done) / float(total))
        return r if r > 1 else 1

    def get_sjob_stat(self,field,statistic):
        """
        Aggregates a node successful job's field using a statistic
        :param field: (str) name of a nodes's field.  ex: wall_time or avg_rss_mem
        :param statistic: (str) choose from ['Avg','Sum','Max','Min','Count']
        
        >>> batch.get_stat('wall_time','Avg')
        120
        """
        if statistic not in ['Avg','Sum','Max','Min','Count']:
            raise ValidationError('Statistic {0} not supported'.format(statistic))
        aggr_fxn = getattr(models, statistic)
        aggr_field = '{0}__{1}'.format(field,statistic.lower())
        return JobAttempt.objects.filter(successful=True,node_set__in = Node.objects.filter(batch=self)).aggregate(aggr_fxn(field))[aggr_field]
    
    def get_node_stat(self,field,statistic):
        """
        Aggregates a node's field using a statistic
        :param field: (str) name of a nodes's field.  ex: cpu_req, mem_req
        :param statistic: (str) choose from ['Avg','Sum','Max','Min','Count']
        
        >>> batch.get_stat('cpu_requirement','Avg')
        120
        """
        if statistic not in ['Avg','Sum','Max','Min','Count']:
            raise ValidationError('Statistic {0} not supported'.format(statistic))
        aggr_fxn = getattr(models, statistic)
        aggr_field = '{0}__{1}'.format(field,statistic.lower())
        r = Node.objects.filter(batch=self).aggregate(aggr_fxn(field))[aggr_field]
        return int(r) if r else r
        

    @property
    def file_size(self):
        "Size of the batch's output_dir"
        return folder_size(self.output_dir)
    
    @property
    def wall_time(self):
        """Time between this batch's creation and finished datetimes.  Note, this is a timedelta instance, not seconds"""
        return self.finished_on - self.created_on if self.finished_on else timezone.now().replace(microsecond=0) - self.created_on
    
    @property
    def output_dir(self):
        "Absolute path to this batch's output_dir"
        return os.path.join(self.workflow.output_dir,self.name)
    
    @property
    def nodes(self):
        "Queryset of this batch's nodes"
        return Node.objects.filter(batch=self)
    
    @property
    def node_edges(self):
        """Edges in this Batch"""
        return NodeEdge.objects.filter(parent__in=self.nodes)
    
    @property
    def num_nodes(self):
        "The number of nodes in this batch"
        return Node.objects.filter(batch=self).count()
    
    @property
    def num_nodes_successful(self):
        "Number of successful nodes in this batch"
        return Node.objects.filter(batch=self,successful=True).count()
    
    def get_all_tag_keywords_used(self):
        """Returns a set of all the keyword tags used on any node in this batch"""
        return map(lambda x: x['key'],NodeTag.objects.filter(node__in=self.nodes).values('key').distinct())
        
    def yield_node_resource_usage(self):
        """
        :yields: (list of tuples) tuples contain resource usage and tags of all nodes.  The first element is the name, the second is the value.
        """
        #TODO rework with time fields
        for node in self.nodes: 
            sja = node.get_successful_jobAttempt()
            if sja: 
                yield [jru for jru in sja.resource_usage_short] + node.tags.items() #add in tags to resource usage tuples
        
    def add_node(self, name, pcmd, outputs={}, hard_reset=False, tags = {}, parents=[], save=True, mem_req=0, cpu_req=1, time_limit=None):
        """
        Adds a node to the batch.  If the node with this name (in this Batch) already exists and was successful, just return the existing one.
        If the existing node was unsuccessful, delete it and all of its output files, and return a new node.
        
        :param name: (str) The name of the node.  Must be unique within this batch. All spaces are converted to underscores.  Required.
        :param pcmd: (str) The preformatted command to execute.  Usually includes the special keywords {output_dir} and {outputs[key]} which will be automatically parsed. Required.
        :param outputs: (dict) a dictionary of outputs and their names. Optional.
        :param hard_reset: (bool) Deletes this node and all associated files and start it fresh. Optional.
        :param tags: (dict) A dictionary keys and values to tag the node with.  These tags can later be used by methods such as :py:meth:`~Workflow.models.Batch.group_nodes_by` and :py:meth:`~Workflow.models.Batch.get_nodes_by` Optional.
        :param save: (bool) If False, will not save the node to the database.  Used in concert with :method:`Workflow.bulk_save_nodes`
        :param parents: (list) A list of parent nodes that this node is dependent on.  This is optional and only used by the DAG functionality.
        :param mem_req: (int) How much memory to reserve for this node in MB. Optional.
        :param cpu_req: (int) How many CPUs to reserve for this node. Optional.
        :param time_limit: (datetime.time) Not implemented.
        
        :returns: If save=True, an instance of a Node.   If save=False, returns (node,tags) where node is a Node, tags is a dict, and node_exists is a bool.
        """
        #validation
        
#        name = re.sub("\s","_",name) #user convenience
#        
#        if name == '' or name is None:
#            raise ValidationError('name cannot be blank')
        if pcmd == '' or pcmd is None:
            raise ValidationError('pre_command cannot be blank')
        
        node_kwargs = {
                       'batch':self,
                       'name':None,
                       'tags':tags,
#                       'name':name,
                       'pre_command':pcmd,
                       'outputs':outputs,
                       'memory_requirement':mem_req,
                       'cpu_requirement':cpu_req,
                       'time_limit':time_limit
                       }
        
        
#        node_exists = Node.objects.filter(batch=self,name=name).count() > 0
        node_exists = Node.objects.filter(batch=self,tags=tags).count() > 0
        if node_exists:
#            node = Node.objects.get(batch=self,name=name)
            node = Node.objects.get(batch=self,tags=tags)
        
        #delete if hard_reset
        if hard_reset:
            if not node_exists:
                raise ValidationError("Cannot hard_reset node with name {0} as it doesn't exist.".format(name))
            node.delete()
        
        if node_exists and not node.successful:
            self.log.info("{0} was unsuccessful last time, deleting old one and trying again".format(node))
            node.delete()
               
        if not node_exists:
            if save:
                #Create and save a node
                node = Node.create(**node_kwargs)
                for k,v in tags.items():
                    NodeTag.objects.create(node=node,key=k,value=v)
                for n in parents:
                    NodeEdge.objects.create(parent=n,child=node,tags=tags) #TODO think about what a NodeEdge tag is
                self.log.info("Created {0} in {1}, and saved to the database.".format(node,self))
            else:
                #Just instantiate a node
                node = Node(**node_kwargs)
        
        #validation
        if node_exists and node.successful:  
            if node.pre_command != pcmd:
                self.log.error("You can't change the pcmd of a existing successful node (keeping the one from history).  Use hard_reset=True if you really want to do this.")
            if node.outputs != outputs:
                self.log.error("You can't change the outputs of an existing successful node (keeping the one from history).  Use hard_reset=True if you really want to do this.")
        
        if not node_exists and not save:
            #if adding to a finished batch, set that batch's status to in_progress so new nodes are executed.  Skip if Node is not being saved.
            batch = node.batch
            if batch.is_done():
                batch.status = 'in_progress'
                batch.successful = False
                batch.save()
        
        #this error should never occur?
#        elif not created and not self.successful:
#            if self.workflow.resume_from_last_failure and node.successful:
#                self.log.error("Loaded successful node {0} in unsuccessful batch {0} from history.".format(node,node.batch))

        if save:
#            node.tag(**tags)
            return node
        else:
            return {'node':node,'tags':tags,'parents':parents,'node_exists':node_exists}


    def is_done(self):
        """
        :returns: True if this batch is finished successfully or failed, else False
        """
        return self.status == 'successful' or self.status == 'failed'

    def _are_all_nodes_done(self):
        """
        :returns: True if all nodes have succeeded or failed in this batch, else False
        """
        return self.nodes.filter(Q(status = 'successful') | Q(status='failed')).count() == self.nodes.count()
        
    def _has_finished(self):
        """
        Executed when this batch has completed running.
        All it does is sets status as either failed or successful
        """
        num_nodes = Node.objects.filter(batch=self).count()
        num_nodes_successful = self.num_nodes_successful
        num_nodes_failed = Node.objects.filter(batch=self,status='failed').count()
        if num_nodes_successful == num_nodes:
            self.successful = True
            self.status = 'successful'
            self.log.info('Batch {0} successful!'.format(self))
        elif num_nodes_failed + num_nodes_successful == num_nodes:
            self.status='failed'
            self.log.warning('Batch {0} failed!'.format(self))
        else:
            #jobs are not done so this shouldn't happen
            raise Exception('Batch._has_finished() called, but not all nodes are completed.')
        
        self.finished_on = timezone.now()
        self.save()
    
    def get_nodes_by(self,tags={},op='and'):
        """
        An alias for :func:`Workflow.get_nodes_by` with batch=self
        
        :returns: a queryset of filtered nodes
        """
        return self.workflow.get_nodes_by(batch=self, tags=tags, op=op)
    
    def get_node_by(self,tags={},op='and'):
        """
        An alias for :func:`Workflow.get_node_by` with batch=self
        
        :returns: a queryset of filtered nodes
        """
        return self.workflow.get_node_by(batch=self, op=op, tags=tags)
                
    def group_nodes_by(self,keys=[]):
        """
        Yields nodes, grouped by tags in keys.  Groups will be every unique set of possible values of tags.
        For example, if you had nodes tagged by color, and shape, and you ran func:`batch.group_nodes_by`(['color','shape']),
        this function would yield the group of nodes that exist in the various combinations of `colors` and `shapes`.
        So for example one of the yields might be (({'color':'orange'n'shape':'circle'}), [ orange_circular_nodes ])
        
        :param keys: The keys of the tags you want to group by.
        :yields: (a dictionary of this group's unique tags, nodes in this group).
        
        .. note:: a missing tag is considered as None and thus placed into a 'None' group with other untagged nodes.  You should generally try to avoid this scenario and have all nodes tagged by the keywords you're grouping by.
        """
        if keys == []:
            yield {},self.nodes
        else:
            node_tag_values = NodeTag.objects.filter(node__in=self.nodes, key__in=keys).values() #get this batch's tags
            #filter out any nodes without all keys
            
            node_id2tags = {}
            for node_id, ntv in helpers.groupby(node_tag_values,lambda x: x['node_id']):
                node_tags = dict([ (n['key'],n['value']) for n in ntv ])
                node_id2tags[node_id] = node_tags
            
            for tags,node_id_and_tags_tuple in helpers.groupby(node_id2tags.items(),lambda x: x[1]):
                node_ids = [ x[0] for x in node_id_and_tags_tuple ]
                yield tags, Node.objects.filter(pk__in=node_ids)
    
    def delete(self, *args, **kwargs):
        """
        Bulk deletes this batch and all files associated with it.
        """
        self.log.info('Deleting Batch {0}.'.format(self.name))
        if os.path.exists(self.output_dir):
            self.log.info('Deleting directory {0}...'.format(self.output_dir))
            os.system('rm -rf {0}'.format(self.output_dir))
        self.log.info('Bulk deleting JobAttempts...')
        JobAttempt.objects.filter(node_set__in = self.nodes).delete()
        self.log.info('Bulk deleting Nodes...')
        self.nodes.delete()
        super(Batch, self).delete(*args, **kwargs)
        self.log.info('{0} Deleted.'.format(self))
    
    @models.permalink    
    def url(self):
        "The URL of this batch"
        return ('batch_view',[str(self.id)])
        
    def __str__(self):
        return 'Batch[{0}] {1}'.format(self.id,re.sub('_',' ',self.name))
            

class NodeTag(models.Model):
    """
    A SQL row that duplicates the information of Node.tags that can be used for filtering, etc.
    """
    "A keyword/value object used to describe a node."
    node = models.ForeignKey('Node')
    key = models.CharField(max_length=63)
    value = models.CharField(max_length=255)
    
    
    def __str__(self):
        return "NodeTag[self.id] {self.key}: {self.value} for Node[{node.id}]".format(self=self,node=self.node)

class NodeEdge(models.Model):
    parent = models.ForeignKey('Node',related_name='parent_edge_set')
    child = models.ForeignKey('Node',related_name='child_edge_set')
    tags = PickledObjectField(null=True,default={})
    "The keys associated with the relationship.  ex, the group_by parameter of a many2one" 
    
    
    def __str__(self):
        return "{0.parent}->{0.child}".format(self)

class Node(models.Model):
    """
    The object that represents the command line that gets executed.
    """
    _jobAttempts = models.ManyToManyField(JobAttempt,related_name='node_set')
    pre_command = models.TextField(help_text='preformatted command.  almost always will contain the special string {output} which will later be replaced by the proper output path')
    exec_command = models.TextField(help_text='the actual command that was executed',null=True)
    name = models.CharField(max_length=255,null=True)
    memory_requirement = models.IntegerField(help_text="Memory to reserve for jobs in MB",default=0,null=True)
    cpu_requirement = models.SmallIntegerField(help_text="Number of CPUs to reserve for this job",default=1)
    time_limit = models.TimeField(help_text="Maximum time for a job to run",default=None,null=True)
    batch = models.ForeignKey(Batch,null=True)
    successful = models.BooleanField(null=False)
    status = models.CharField(max_length=100,choices = status_choices,default='no_attempt')
    outputs = PickledObjectField(null=True) #dictionary of outputs
    
    tags = PickledObjectField(null=False)
    created_on = models.DateTimeField(null=True,default=None)
    finished_on = models.DateTimeField(null=True,default=None)
    
    
    def __init__(self, *args, **kwargs):
        kwargs['created_on'] = timezone.now()
        super(Node,self).__init__(*args, **kwargs)
    
    @staticmethod
    def create(*args,**kwargs):
        node = Node.objects.create(*args,**kwargs)
        
        if Node.objects.filter(batch=node.batch,tags=node.tags).count() > 1:
            node.delete()
            raise ValidationError("Nodes belonging to a batch with the same tags detected! tags: {0}".format(node.tags))
        
        check_and_create_output_dir(node.output_dir)
        check_and_create_output_dir(node.job_output_dir) #this is not in JobManager because JobMaster should be not care about these details
            
        #Create node tags    
        if type(node.tags) == dict:
            for key,value in node.tags.items():
                NodeTag.objects.create(node=node,key=key,value=value)
                
        return node
    
    @property
    def workflow(self):
        "This node's workflow"
        return self.batch.workflow

    @property
    def parents(self):
        "This node's parents"
        return map(lambda n: n.parent, NodeEdge.objects.filter(child=self).all())

    @property
    def log(self):
        "This node's workflow's log"
        return self.workflow.log

    @property
    def file_size(self,human_readable=True):
        "Node filesize"
        return folder_size(self.output_dir,human_readable=human_readable)
    
    @property
    def output_dir(self):
        "Node output dir"
        return os.path.join(self.batch.output_dir,str(self.id))
    
    @property
    def job_output_dir(self):
        """Where the job output goes"""
        return os.path.join(self.output_dir,'out')
    
    @property
    def output_paths(self):
        "Dict of this node's outputs appended to this node's output_dir."
        r = {}
        for key,val in self.outputs.items():
            r[key] = os.path.join(self.job_output_dir,val)
        return r
    
    @property
    def jobAttempts(self):
        "Queryset of this node's jobAttempts."
        return self._jobAttempts.all().order_by('id')
    
    @property
    def wall_time(self):
        "Node's wall_time"
        return self.get_successful_jobAttempt().wall_time if self.successful else None
    
    def numAttempts(self):
        "This node's number of job attempts."
        return self._jobAttempts.count()
    
    def get_successful_jobAttempt(self):
        """
        Get this node's successful job attempt.
        
        :return: this node's successful job attempt.  If there were no successful job attempts, returns None
        """
        jobs = self._jobAttempts.filter(successful=True)
        if len(jobs) == 1:
            return jobs[0]
        elif len(jobs) > 1:
            raise Exception('more than 1 successful job, something went wrong!')
        else:
            return None # no successful jobs


    def _has_finished(self,jobAttempt):
        """
        Executed whenever this node finishes by the workflow.
        
        Sets self.status to 'successful' or 'failed' and self.finished_on to 'current_timezone'
        Will also run self.batch._has_finished() if all nodes in the batch are done.
        """
        if self._jobAttempts.filter(successful=True).count():
            self.status = 'successful'
            self.successful = True
            self.log.info("{0} Successful!".format(self,jobAttempt))        
        else:
            self.status = 'failed'
            self.log.info("{0} Failed!".format(self,jobAttempt))
            
        self.finished_on = timezone.now()
        self.save()
        
        if self.batch._are_all_nodes_done(): self.batch._has_finished()    
 
#    def __tag(self,tags):
#        """
#        Tag this node with key value pairs.  Should only be called by Node.__init__
#        
#        :param tags: (dict) the node's tags
#        """
#        #TODO don't allow tags called things like 'status' or other node attributes
#        for key,value in tags.items():
#            NodeTag.objects.create(node=self,key=key,value=value)
        
#    def tag(self,**kwargs):
#        """
#        Tag this node with key value pairs.  If the key already exists, its value will be overwritten.
#        
#        >>> node.tag(color="blue",shape="circle")
#        """
#        #TODO don't allow tags called things like 'status' or other node attributes
#        for key,value in kwargs.items():
#            value = str(value)
#            nodetag, created = NodeTag.objects.get_or_create(node=self,key=key,defaults= {'value':value})
#            if not created:
#                nodetag.value = value
#            nodetag.save()

#    @property            
#    def tags(self):
#        """
#        The dictionary of this node's tags.
#        """
#        return dict([(x['key'],x['value']) for x in self.nodetag_set.all().values('key','value')])
    
    @models.permalink    
    def url(self):
        "This node's url."
        return ('node_view',[str(self.id)])


    def delete(self, *args, **kwargs):
        """
        Deletes this node and all files associated with it
        """
        self.log.info('Deleting node {0} and its output directory {0}'.format(self.name,self.output_dir))
        #todo delete stuff in output_paths that may be extra files
        for ja in self._jobAttempts.all(): ja.delete()
        self.nodetags.delete()
        if os.path.exists(self.output_dir):
            os.system('rm -rf {0}'.format(self.output_dir))
        super(Node, self).delete(*args, **kwargs)
    
    def __str__(self):
        return 'Node[{0}] {1}'.format(self.id,self.tags)



