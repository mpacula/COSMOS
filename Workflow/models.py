from django.db import models
from django.db.models import Q
from JobManager.models import JobAttempt,JobManager
import os,sys,re,logging
from Cosmos.helpers import get_drmaa_ns,validate_name,validate_not_null, check_and_create_output_dir, folder_size, get_workflow_logger
from Cosmos import helpers
from django.core.exceptions import ValidationError
from picklefield.fields import PickledObjectField
from cosmos_session import cosmos_settings
from django.utils import timezone

waiting_on_workflow = None #set to whichever workflow has a wait().  used by ctrl_c to terminate it

status_choices=(
                ('successful','Successful'),
                ('no_attempt','No Attempt'),
                ('in_progress','In Progress'),
                ('failed','Failed')
                )


class Workflow(models.Model):
    """   
    This is the master object.  It contains a list of **Batches** which represent a pool of jobs that have no dependencies on each other
    and can be executed at the same time. 
    """
    name = models.CharField(max_length=250,unique=True)
    output_dir = models.CharField(max_length=250)
    jobManager = models.OneToOneField(JobManager,null=True, related_name='workflow')
    resume_from_last_failure = models.BooleanField(default=False,help_text='resumes from last failed node')
    dry_run = models.BooleanField(default=False,help_text="don't execute anything")
    max_reattempts = models.SmallIntegerField(default=3)
    _terminating = models.BooleanField(default=False,help_text='this workflow is terminating')
    
    created_on = models.DateTimeField(default=timezone.now())
    finished_on = models.DateTimeField(null=True)
    
    
    def __init__(self, *args, **kwargs):
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
        
    
    def terminate(self):
        """
        Terminates this workflow.  Generally executing via the command.py terminate
        command line interface.  qdels all jobs and tells the workflow to terminate
        """
        self.log.warning("Terminating this workflow...")
        self._terminating = True
        self.save()
        ids = [ str(ja.drmaa_jobID) for ja in self.jobManager.jobAttempts.filter(queue_status='queued') ]
        #jobIDs = ', '.join(ids)
        #cmd = 'qdel {0}'.format(jobIDs)
        for id in ids:
            cmd = 'qdel {0}'.format(id)
            os.system(cmd)
        #self.log.warning('Executed {0}'.format(cmd))
    
    def _is_terminating(self):
        """
        Helper function to check if this workflow is terminating.  Accessing the Workflow._terminating
        variable will probably hit a cache and not report the correct value;
        this function forces a database query.
        """
        if Workflow.objects.filter(pk=self.id,_terminating=True).count() > 0: #database can be out of sync
            self._terminating = True
            self.save()
            return True
        return False
        
    @property
    def nodes(self):
        """Nodes in this Workflow"""
        return Node.objects.filter(batch__in=self.batch_set.all())
    
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
    def start(name=None, restart=False, dry_run=False, root_output_dir=None):
        """
        Starts a workflow.  If a workflow with this name already exists, return the workflow.
        
        :param name: A unique name for this workflow
        :param __restart: Restart the workflow by deleting it and creating a new one
        :param dry_run: Don't actually execute jobs
        :param root_output_dir: If not None, sets the root_output_dir.  If None, uses the value in settings.  If the workflow already exists, this param is ignored.
        """

        if name is None:
            raise ValidationError('Name of a workflow cannot be None')
        
        name = re.sub("\s","_",name)
        
        if root_output_dir is None:
            root_output_dir = cosmos_settings.default_root_output_dir
            
        if Workflow.objects.filter(name=name).count()>0:
            if restart:
                return Workflow.__restart(name=name, root_output_dir=root_output_dir, dry_run=dry_run)
            else:
                return Workflow.__resume(name=name, dry_run=dry_run)
        else:
            return Workflow.__create(name=name, dry_run=False, root_output_dir=root_output_dir)
        
    
    @staticmethod
    def __resume(name=None,dry_run=False):
        """
        Resumes a workflow from the last failed node.
        
        :param name: A unique name for this workflow
        :type name: str
        :param dry_run: This workflow is a dry run.  No jobs will actually be executed
        :type dry_run: bool
        :param root_output_dir: Optional override of the root_output_dir contains in the configuration file
        :type root_output_dir: str
        """

        if Workflow.objects.filter(name=name).count() == 0:
            raise ValidationError('Workflow {0} does not exist, cannot resume it'.format(name))
        wf = Workflow.objects.get(name=name)
        wf._terminating=False
        wf.resume_from_last_failure=True
        wf.dry_run=dry_run
        
        wf.save()
        wf.log.info('Resuming this workflow.')
        Batch.objects.filter(workflow=wf).update(order_in_workflow=None)
        
        return wf

    @staticmethod
    def __restart(name=None,root_output_dir=None,dry_run=False):
        """
        Restarts a workflow.  Will delete the old workflow and all of its files
        but will retain the old workflow id for convenience
        
        :param name: A unique name for this workflow
        :type name: str
        :param dry_run: This workflow is a dry run.  No jobs will actually be executed
        :type dry_run: bool
        :param root_output_dir: Optional override of the root_output_dir contains in the configuration file
        :type root_output_dir: str
        """
        old_wf_exists = Workflow.objects.filter(name=name).count() > 0
            
        if old_wf_exists:
            old_wf = Workflow.objects.get(name=name)
            wf_id = old_wf.id
            old_wf.delete(delete_files=True)
        else:
            wf_id=None
        
        new_wf = Workflow.__create(_wf_id=wf_id,name=name,root_output_dir=root_output_dir,dry_run=dry_run)
        
        new_wf.log.info('Restarting this Workflow.')
        
        return new_wf
                
    @staticmethod
    def __create(name=None,dry_run=False,root_output_dir=None,_wf_id=None):
        """
        Creates a new workflow
        
        :param name: A unique name for this workflow
        :type name: str
        :param dry_run: This workflow is a dry run.  No jobs will actually be executed
        :type dry_run: bool
        :param root_output_dir: Optional override of the root_output_dir contains in the configuration file
        :type root_output_dir: str
        """
        
        check_and_create_output_dir(root_output_dir)
        output_dir = os.path.join(root_output_dir,name)
            
        wf = Workflow.objects.create(id=_wf_id,name=name, output_dir=output_dir, dry_run=dry_run)
        wf.save()
        wf.log.info('Created Workflow {0}.'.format(wf))
        return wf
            
        
    def add_batch(self,name, hard_reset=False):
        """
        Adds a batch to this workflow.  If a batch with this name (in this Workflow) already exists, return the existing one.
        
        :parameter name: The name of the batch, must be unique within this Workflow
        :parameter hard_reset: Delete any batch with this name including all of its nodes, and return a new one
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
            if hard_reset:
                self.log.info("Doing a hard reset on batch {0}.".format(name))
                old_batch.delete()
                
        b, created = Batch.objects.get_or_create(workflow=self,name=name,id=_old_id)
        if created:
            self.log.info('Creating {0} from scratch.'.format(b))
        else:
            self.log.info('{0} already exists, loading it from history...'.format(b))
            
        b.order_in_workflow = order_in_workflow
        b.save()
        return b
        
 
    def yield_resource_usage(self):
        "Yield's each node in the Workflow's resource usage"
        yield 'Batch,failures,file_size,memory,time'
        for batch in self.batches:
            for node in batch.nodes:
                sja = node.get_successful_jobAttempt()
                memory = '0'
                utime = 0
                if sja and sja._drmaa_info: #TODO missing this  stuff is caused by errors.  fix those errors and delete this
                        if sja.drmaa_utime: utime = sja.drmaa_utime 
                        try:
                            memory = sja._drmaa_info['resourceUsage']['mem']
                        except KeyError:
                            pass
                fs = helpers.folder_size(node.output_dir,human_readable=False) #TODO make obj.file_size a fxn not a property
                yield '{batch.name},-1,{fs},{memory},{utime}'.format(batch=batch,node=node,fs=fs,memory=memory,sja=sja,utime=utime)
                
     
    def save_resource_usage(self):
        "Saves this workflow's resource usage to disk"
        d = os.path.join(self.output_dir,'_plots')
        with file(d,'wb') as f:
            for line in self.yield_resource_usage():
                f.write(line+"\n")
             
                
#    def _close_session(self):
#        """Shuts down this workflow's jobmanager"""
#        self.log.info('Ending session')
#        self.jobManager._close_session()
            
    def delete(self, *args, **kwargs):
        """
        Deletes this workflow.
        :param delete_files: Deletes all files associated with this workflow
        """
#        self._close_session()
        self.jobManager.delete()
        self.save()
        
        delete_files=False
        if 'delete_files' in kwargs:
            delete_files = kwargs.pop('delete_files')
        
        super(Workflow, self).delete(*args, **kwargs)
        
        #delete_files=True will delete all output files
        if delete_files:
            self.log.info('Deleting directory {0}'.format(self.output_dir))
            for h in self.log.handlers:
                h.close()
            if os.path.exists(self.output_dir):
                os.system('rm -rf {0}'.format(self.output_dir))
                
        
    def __create_command_sh(self,command,file_path):
        """Create a sh script that will execute command"""
        with open(file_path,'wb') as f:
            f.write('#!/bin/sh\n')
            f.write(command)
        os.system('chmod 700 {0}'.format(file_path))

    def _check_termination(self,check_for_leftovers = True):
        """
        Checks if the current workflow is terminating.  If it is terminating, it will
        initiate the termination sequence.
        """
        if self._is_terminating():
            #make sure all jobs get returned
            self.log.warning("Termination sequence initiated.")
            if check_for_leftovers:
                self._check_and_wait_for_leftover_nodes()
            self._clean_up()
            
            self.log.warning("Workflow was terminated, exiting with exit code 2.")
            self._terminating=False
            self.save()
            import sys; sys.exit(2)

    def _run_node(self,node):
        """
        Executes a node and returns a jobAttempt
        """
        self._check_termination()
        
        node.batch.status = 'in_progress'
        node.batch.save()
        node.status = 'in_progress'
        self.log.info('Running {0} from {1}'.format(node,node.batch))
        try:
            node.exec_command = node.pre_command.format(output_dir=node.job_output_dir,outputs = node.outputs)
        except KeyError:
            helpers.formatError(node.pre_command,{'output_dir':node.job_output_dir,'outputs': node.outputs})
                
        #create command.sh that gets executed
        command_script_path = os.path.join(node.output_dir,'command.sh')
        self.__create_command_sh(node.exec_command,command_script_path)
        
        jobAttempt = self.jobManager.addJobAttempt(command_script_path=command_script_path,
                                     drmaa_output_dir=os.path.join(node.output_dir,'drmaa_out/'),
                                     jobName=node.name,
                                     drmaa_native_specification=get_drmaa_ns(cosmos_settings.DRM, node.memory_requirement))
        
        node._jobAttempts.add(jobAttempt)
        if self.dry_run:
            self.log.info('Dry Run: skipping submission of job {0}.'.format(jobAttempt))
        else:
            self.jobManager.submitJob(jobAttempt)
            self.log.info('Submitted jobAttempt with drmaa jobid {0}.'.format(jobAttempt.drmaa_jobID))
        node.save()
        self.jobManager.save()
        return jobAttempt

    def run_batch(self,batch):
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
        Returns True if another jobAttempt was submitted
        Returns False if the max jobAttempts has already been reached
        """
        numAttempts = node.jobAttempts.count()
        if not node.successful: #ReRun jobAttempt
            if self._is_terminating():
                self.log.info("Skipping reattempt since workflow is terminating".format(failed_jobAttempt, node,numAttempts))
                self.status = 'failed'
                self.save()
                return False
            elif numAttempts < self.max_reattempts:
                self.log.warning("JobAttempt {0} of node {1} failed, on attempt # {2}, so deleting failed output files and retrying".format(failed_jobAttempt, node,numAttempts))
                os.system('rm -rf {0}/*'.format(node.job_output_dir))
                self._run_node(node)
                return True
            else:
                self.log.warning("Node {0} has reached max_reattempts of {0}.  This node has failed".format(self, self.max_reattempts))
                self.status = 'failed'
                self.save()
                return False
    
    def __wait(self,batch=None,terminate_on_fail=False):
        """
        Waits for all executing nodes to finish.  Returns an array of the nodes that finished.
        if batch is omitted or set to None, all running nodes will be waited on
        """
        nodes = []
        if batch is None:
            self.log.info('Waiting on all nodes...')
        else:
            self.log.info('Waiting on batch {0}...'.format(batch))
        
        for jobAttempt in self.jobManager.yield_All_Queued_Jobs():
            node = jobAttempt.node
            #self.log.info('Finished {0} for {1} of {2}'.format(jobAttempt,node,node.batch))
            nodes.append(node)
            if jobAttempt.successful:
                node._has_finished(jobAttempt)
            else:
                submitted_another_job = self._reattempt_node(node,jobAttempt)
                if not submitted_another_job:
                    node._has_finished(jobAttempt) #job has failed and out of reattempts
                    if not self._is_terminating() and terminate_on_fail:
                        self.log.warning("{0} has reached max_reattempts and terminate_on_fail==True so terminating.".format(node))
                        self.terminate()
            
            if node.batch._are_all_nodes_done():
                node.batch._has_finished()
                break
                    
        if batch is None: #no waiting on a batch
            self.log.info('All nodes for this wait have completed!')
        else:
            self.log.info('All nodes for this wait on {0} completed!'.format(batch))
        
        waiting_on_workflow = None
        return nodes 


    def wait(self, batch=None, terminate_on_fail=False):
        """
        Waits for all executing nodes to finish.  Returns an array of the nodes that finished.
        If `batch` is omitted or set to None, all running nodes will be waited on.
        
        :param terminate_on_fail: If True, the workflow will self terminate of any of the nodes of this batch fail `max_job_attempts` times
        """
        nodes = self.__wait(batch=batch,terminate_on_fail=terminate_on_fail)
        self._check_termination(check_for_leftovers=True) #this is why there's a separate __wait() - so check_for_leftovers can call __wait() and not wait().  Otherwise checking for leftovers would loop forever!
        return nodes

    def run_wait(self,batch):
        """
        shortcut to run_batch(); wait(batch=batch);
        """
        self.run_batch(batch=batch)
        return self.wait(batch=batch)

    def finished(self,delete_unused_batches=True):
        """
        Call at the end of every workflow.
        If there any left over jobs that have not been collected,
        It will wait for all of them them
        
        :param delete_unused_batches: Any batches and their output_dir from previous workflows that weren't loaded since the last create, __resume, or __restart, using add_batch() are deleted.
        
        """
        self._check_and_wait_for_leftover_nodes()
        self._clean_up(delete_unused_batches=delete_unused_batches)
        self.finished_on = timezone.now()
        self.save()
        self.log.info('Finished.')
        
    def _check_and_wait_for_leftover_nodes(self):
        """Checks and waits for any leftover nodes"""
        if self.nodes.filter(status='in_progress').count()>0:
            self.log.warning("There are left over nodes in the queue, waiting for them to finish")
            self.__wait()
        
    def _clean_up(self,delete_unused_batches=False):
        """
        Should be executed at the end of a workflow.
        """
        self.log.debug("Cleaning up workflow")
        if delete_unused_batches:
            Batch.objects.filter(workflow=self,order_in_workflow=None).delete() #these batches weren't used again after a __restart
               
#        if self._is_terminating():
#            for batch in self.batch_set.filter(status='in_progress').all():
#                batch.status = 'failed'
#                batch.save()
    
    def restart_from_here(self):
        """
        Deletes any batches in the history that haven't been added yet
        """
        self.log.info('Restarting Workflow from here.')
        Batch.objects.filter(workflow=self,order_in_workflow=None).delete()
    
    def __str__(self):
        return 'Workflow[{0}] {1}'.format(self.id,re.sub('_',' ',self.name))
            
    def toString(self):
        s = 'Workflow[{0.id}] {0.name} resume_from_last_failure={0.resume_from_last_failure}\n'.format(self)
        #s = '{:*^72}\n'.format(s) #center s with stars around it
        for batch in self.batch_set.all():
            s = s + batch.toString(tabs=1)
        return s
    
    @models.permalink    
    def url(self):
        return ('workflow_view',[str(self.id)])
    
    def get_nodes_by(self,batch=None,op="and",**kwargs):
        """
        Returns the list of nodes that are tagged by the keys and vals in **kwargs dictionary
        
        :param op: Choose either 'and' or 'or' as the logic to filter tags with
        :param **kwargs: Any keywords you'd like to search for
        :returns: a query result of the filtered nodes
        
        >>> node.get_nodes_by(op='or',color='grey',color='orange')
        >>> node.get_nodes_by(op='and',color='grey',shape='square')
        """
        
        if op == 'or':
            raise NotImplemented('sorry')
        
        if batch:
            nodes = batch.nodes
        else:
            nodes = self.nodes
            
        alltags = NodeTag.objects.filter(node__in=nodes)
            
        for k,v in kwargs.items():
            nodes = nodes.filter(nodetag__in=alltags.filter(key=k,value=v))
            
        return nodes


    def get_node_by(self,batch=None,op="and",**kwargs):
        """
        Returns the list of nodes that are tagged by the keys and vals in **kwargs dictionary.
        
        :raises Exception: if more or less than one node is returned
        
        :param op: Choose either 'and' or 'or' as the logic to filter tags with
        :param **kwargs: Any keywords you'd like to search for
        :returns: a query result of the filtered nodes
        
        >>> node.get_node_by(op='or',color='grey',color='orange')
        >>> node.get_node_by(op='and',color='grey',shape='square')
        """
    
        nodes = self.get_nodes_by(batch=batch,op=op,**kwargs)
        n = nodes.count()
        if n>1:
            raise Exception("More than one node with tags {0}".format(kwargs))
        elif n == 0:
            raise Exception("No nodes with with tags {0}.".format(kwargs))
        return nodes[0]
    
class Batch(models.Model):
    """
    A group of jobs that can be run independently.  See `Embarassingly Parallel <http://en.wikipedia.org/wiki/Embarrassingly_parallel>`_ .
    
    .. note:: A Batch should not be directly created.  Use ``workflow.add_batch()`` to create a new batch.
    A Batch is `Embarrassingly Parallel <http://en.wikipedia.org/wiki/Embarrassingly_parallel>`_.
    """
    name = models.CharField(max_length=200)
    workflow = models.ForeignKey(Workflow)
    order_in_workflow = models.IntegerField(null=True)
    status = models.CharField(max_length=200,choices=status_choices,default='no_attempt') 
    successful = models.BooleanField(default=False)
    created_on = models.DateTimeField(default=timezone.now())
    finished_on = models.DateTimeField(null=True,default=None)
    
    
    def __init__(self,*args,**kwargs):
        super(Batch,self).__init__(*args,**kwargs)
        validate_not_null(self.workflow)
        if self.id is None: #creating for the first time 
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
        total = self.numNodes()
        status = self.status
        if total == 0 or done == 0:
            if status == 'in_progress' or status == 'failed':
                return 1
            return 0
        return int(100 * float(done) / float(total))
    
    @property
    def max_job_time(self):
        "Max job time of all jobs in this batch"
        m = JobAttempt.objects.filter(node_set__in = Node.objects.filter(batch=self)).aggregate(models.Max('drmaa_utime'))['drmaa_utime__max']
        if m is None:
            return None
        return m
    
    @property
    def total_job_time(self):
        "Total job time of all jobs in this batch"
        t = JobAttempt.objects.filter(node_set__in = Node.objects.filter(batch=self)).aggregate(models.Sum('drmaa_utime'))['drmaa_utime__sum']
        if t is None:
            return None
        return t
    
    @property
    def file_size(self):
        "Size of the batch's output_dir"
        return folder_size(self.output_dir)
    
    @property
    def output_dir(self):
        "Absolute path to this batch's output_dir"
        return os.path.join(self.workflow.output_dir,self.name)
    
    @property
    def nodes(self):
        "Queryset of this batch's nodes"
        return Node.objects.filter(batch=self)
    
    def numNodes(self):
        "The number of nodes in this batch"
        return Node.objects.filter(batch=self).count()
    
    def add_node(self, name, pcmd, outputs={}, hard_reset=False, tags = {}, mem_req=0, time_limit=None):
        """
        Adds a node to the batch.  If the node with this name (in this Batch) already exists and was successful, just return the existing one.
        If the existing node was unsuccessful, delete it and all of its output files, and return a new node.
        
        .. note:: all spaces are converted to underscores
        
        :param name: (str) The name of the node.  Must be unique within this batch.
        :param pcmd: (str) The preformatted command to execute. Required.
        :param outputs: (dict) a dictionary of outputs and their names. Optional.
        :param hard_reset: (bool) Deletes this node and all associated files and start it fresh. Optional.
        :param tags: (dict) any other keyword arguments will add a :class:`NodeTag` to the node. Optional.
        :param mem_req: (int) Optional setting to tell the DRM how much memory to reserve in MB. Optional.
        :param time_limit: (datetime.time) any other keyword arguments will add `tag`s to the node. Optional.
        """
        #TODO need a dict for special outputs?
        #validation
        
        name = re.sub("\s","_",name) #user convenience
        
        if name == '' or name is None:
            raise ValidationError('name cannot be blank')
        if pcmd == '' or pcmd is None:
            raise ValidationError('pre_command cannot be blank')
        
        
        node_exists = Node.objects.filter(batch=self,name=name).count() > 0
        if node_exists:
            node = Node.objects.get(batch=self,name=name)
        
        #delete if hard_reset
        if hard_reset:
            if not node_exists:
                raise ValidationError("Cannot hard_reset node with name {0} as it doesn't exist.".format(name))
            node.delete()
        
        if node_exists and (not node.successful) and self.workflow.resume_from_last_failure:
            self.log.info("Node was unsuccessful last time, deleting old one and trying again {0}".format(node))
            node.delete()
               
        node,created = Node.objects.get_or_create(batch=self,name=name,defaults={'pre_command':pcmd,'outputs':outputs,'memory_requirement':mem_req,'time_limit':time_limit})
        
        #validation
        if (not created) and node.successful:  
            if node.pre_command != pcmd:
                self.log.error("You can't change the pcmd of a existing successful node (keeping the one from history).  Use hard_reset=True if you really want to do this.")
            if node.outputs != outputs:
                self.log.error("You can't change the outputs of an existing successful node (keeping the one from history).  Use hard_reset=True if you really want to do this.")
        
        if created:
            self.log.info("Created node {0} from scratch.".format(node))
            #if adding to a finished batch, set that batch's status to in_progress so new nodes are executed
            batch = node.batch
            if batch.is_done():
                batch.status = 'in_progress'
                batch.successful = False
                batch.save()
        
        #this error should never occur    
        elif not created and not self.successful:
            if self.workflow.resume_from_last_failure and node.successful:
                self.log.error("Loaded successful node {0} in unsuccessful batch {0} from history.".format(node,node.batch))

        node.save()
        node.tag(**tags)
            
        return node


    def is_done(self):
        """
        Returns True if this batch is finished successfully or failed
        """
        return self.status == 'successful' or self.status == 'failed'

    def _are_all_nodes_done(self):
        """
        Returns True if all nodes have succeeded or failed in this batch
        """
        return self.nodes.filter(Q(status = 'successful') | Q(status='failed')).count() == self.nodes.count()
        
    def _has_finished(self):
        """
        Executed when this batch has completed running.
        All it does is sets status as either failed or successful
        """
        num_nodes = Node.objects.filter(batch=self).count()
        num_nodes_successful = Node.objects.filter(batch=self,successful=True).count()
        num_nodes_failed = Node.objects.filter(batch=self,status='failed').count()
        if num_nodes_successful == num_nodes:
            self.successful = True
            self.status = 'successful'
            self.log.info('Batch {0} successful!'.format(self))
        elif num_nodes_failed + num_nodes_successful == num_nodes:
            self.status='failed'
            self.log.warning('Batch {0} failed!'.format(self))
        
        self.finished_on = timezone.now()
        self.save()
    
    def get_nodes_by(self,op='and',**kwargs):
        """
        An alias for :func:`Workflow.get_nodes_by` with batch=self
        
        :returns: a queryset of filtered nodes
        """
        return self.workflow.get_nodes_by(batch=self, op=op, **kwargs)
    
    def get_node_by(self,op='and',**kwargs):
        """
        An alias for :func:`Workflow.get_node_by` with batch=self
        
        :returns: a queryset of filtered nodes
        """
        return self.workflow.get_node_by(batch=self, op=op, **kwargs)
                
    def group_nodes_by(self,*args):
        """
        Yields nodes, grouped by tags in *args.  Groups will be every unique set of possible values of tags.
        For example, if you had nodes tagged by color, and shape, and you ran func:`batch.group_nodes_by`('color','shape'),
        this function would yield the group of nodes that exist in the various combinations of ``color``s and ``shape``s.
        So one of the yields may be (({'color':'orange'n'shape':'circle'}), [ orange_circular_nodes ])
        
        :param *args: Keywords of the tags you want to group by
        :yields: a tuple of (tags_in_this_group,nodes in group)
        
        .. note:: a missing tag is considered as None and thus its own grouped with other similar nodes.  You should generally
        try to avoid this scenario and have all nodes tagged by the keywords you're grouping by.
        
        .. note:: any nodes without a single one of the tags in *args are not included.
        """
        node_tag_values = NodeTag.objects.filter(node__in=self.nodes, key__in=args).values() #get this batch's tags
        #filter out any nodes without all args
        
        node_id2tags = {}
        for node_id, ntv in helpers.groupby(node_tag_values,lambda x: x['node_id']):
            node_tags = dict([ (n['key'],n['value']) for n in ntv ])
            node_id2tags[node_id] = node_tags
        
        for tags,node_id_and_tags_tuple in helpers.groupby(node_id2tags.items(),lambda x: x[1]):
            node_ids = [ x[0] for x in node_id_and_tags_tuple ]
            yield tags, Node.objects.filter(pk__in=node_ids)
    
    
    
    def delete(self, *args, **kwargs):
        """
        Deletes this batch and all files associated with it.
        """
        self.log.debug('Deleting Batch {0}'.format(self.name))
        self.nodes.all().delete()
        if os.path.exists(self.output_dir):
            os.system('rm -rf {0}'.format(self.output_dir))
        super(Batch, self).delete(*args, **kwargs)
    
    @models.permalink    
    def url(self):
        "The URL of this batch"
        return ('batch_view',[str(self.id)])
        
    def __str__(self):
        return 'Batch[{0}] {1}'.format(self.id,re.sub('_',' ',self.name))
    
    def toString(self,tabs=0):
        # s = ['-'*72]
        s = ['{tabs}Batch[{1}] {0}'.format(self.name,self.id,tabs="  "*tabs)]
        #s = ['-'*72]
        for node in self.nodes.all():
            node_str = '  '*(tabs+1)+node.toString()
            node_str = node_str.replace("\n","{0}{1}".format('\n',"  "*(tabs+2)))
            s.append(node_str)
        return '\n'.join(s)
            

class NodeTag(models.Model):
    "A keyword/value object used to describe a node."
    node = models.ForeignKey('Node')
    key = models.CharField(max_length=63)
    value = models.CharField(max_length=255)
    
    
    def __str__(self):
        return "Tag({0}) - {0}: {0}".format(self.node, self.key, self.value)

class Node(models.Model):
    """
    The object that represents the command line that gets executed.
    """
    _jobAttempts = models.ManyToManyField(JobAttempt,related_name='node_set')
    pre_command = models.TextField(help_text='preformatted command.  almost always will contain the special string {output} which will later be replaced by the proper output path')
    exec_command = models.TextField(help_text='the actual command that was executed',null=True)
    name = models.CharField(max_length=255,null=True)
    memory_requirement = models.IntegerField(help_text="Memory to reserve for jobs in MB",default=None,null=True)
    time_limit = models.TimeField(help_text="Maximum time for a job to run",default=None,null=True)
    batch = models.ForeignKey(Batch,null=True)
    successful = models.BooleanField(null=False)
    status = models.CharField(max_length=100,choices = status_choices,default='no_attempt')
    outputs = PickledObjectField(null=True) #dictionary of outputs
    
    created_on = models.DateTimeField(default=timezone.now())
    finished_on = models.DateTimeField(null=True,default=None)
    
    
    def __init__(self, *args, **kwargs):
        super(Node,self).__init__(*args, **kwargs)
        validate_name(self.name)
        if self.id is None:
            if Node.objects.filter(batch=self,name=kwargs['name']).count() > 0:
                raise ValidationError("Nodes belonging to a batch with the same name detected!".format(self.name))
        if self.id is None: #creating for the first time
            check_and_create_output_dir(self.output_dir) 
            check_and_create_output_dir(self.job_output_dir)
    
    @property
    def workflow(self):
        "This node's workflow"
        return self.batch.workflow
    
    @property
    def nodetags(self):
        "Queryset of NodeTag Objects"
        return NodeTag.objects.filter(node=self)

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
        return os.path.join(self.batch.output_dir,self.name)
    
    @property
    def job_output_dir(self):
        """Where the job output goes"""
        return os.path.join(self.output_dir,'out')
    
    @property
    def output_paths(self):
        "Dictionary of this node's outputs appended to this node's output_dir."
        r = {}
        for key,val in self.outputs.items():
            r[key] = os.path.join(self.job_output_dir,val)
        return r
    
    @property
    def jobAttempts(self):
        "Queryset of this node's jobAttempts."
        return self._jobAttempts.all().order_by('id')
    
    @property
    def time_to_run(self):
        "Time it took this node to run."
        return self.get_successful_jobAttempt().drmaa_utime if self.successful else None
    
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
        
    def tag(self,**kwargs):
        """
        Tag this node with key value pairs.  If the key already exists, it will be overwritten.
        
        >>> node.tag(color="blue",shape="circle")
        """
        #TODO don't allow tags called things like 'status' or other node attributes
        for key,value in kwargs.items():
            value = str(value)
            nodetag, created = NodeTag.objects.get_or_create(node=self,key=key,defaults= {'value':value})
            if not created:
                nodetag.value = value
            nodetag.save()

    @property            
    def tags(self):
        """
        The dictionary of this node's tags.
        """
        return dict([(x['key'],x['value']) for x in self.nodetag_set.all().values('key','value')])
    
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
        self._jobAttempts.all().delete()
        self.nodetags.delete()
        if os.path.exists(self.output_dir):
            os.system('rm -rf {0}'.format(self.output_dir))
        super(Node, self).delete(*args, **kwargs)
    
    def __str__(self):
        return 'Node[{0}] {1}'.format(self.id,re.sub('_',' ',self.name))
        
    def toString(self):
        drmaa_stdout = '' #default if job is unsuccessful
        drmaa_stderr = '' #default if job is unsuccessful
        if self.successful:
            j = self.get_successful_jobAttempt()
            drmaa_stdout = j.get_drmaa_STDOUT_filepath()
            drmaa_stderr = j.get_drmaa_STDERR_filepath()
        return ("Node[{self.id}] {self.name}:\n"
        "Belongs to batch {batch}:\n"
        "exec_command: \"{self.exec_command}\"\n"
        "successful: {successful}\n"
        "status: {status}\n"
        "attempts: {attempts}\n"
        "time_to_run: {self.time_to_run}\n"
        "drmaa_stdout:\n"
        "drmaa_stderr:").format(self=self,
                                batch=self.batch,
                                status=self.status(),
                                attempts=self.numAttempts(),
                                successful=self.successful,
                                drmaa_stdout=drmaa_stdout,
                                drmaa_stderr=drmaa_stderr)



