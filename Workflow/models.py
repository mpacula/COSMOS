from django.db import models
from django.db.models import Q
from JobManager.models import JobAttempt,JobManager
import os,sys,re,logging
from Cosmos.helpers import get_rusage,validate_name,validate_not_null, check_and_create_output_dir, folder_size, get_workflow_logger
from Cosmos import helpers
from django.core.exceptions import ValidationError
from picklefield.fields import PickledObjectField
from cosmos_session import cosmos_settings

waiting_on_workflow = None #set to whichever workflow has a wait().  used by ctrl_c to terminate it

status_choices=(
                ('successful','Successful'),
                ('no_attempt','No Attempt'),
                ('in_progress','In Progress'),
                ('failed','Failed')
                )


class Workflow(models.Model):
    """
    A collection of Batches of Jobs
    """
    name = models.CharField(max_length=250,unique=True)
    output_dir = models.CharField(max_length=250)
    jobManager = models.OneToOneField(JobManager,null=True, related_name='workflow')
    resume_from_last_failure = models.BooleanField(default=False,help_text='resumes from last failed node')
    dry_run = models.BooleanField(default=False,help_text="don't execute anything")
    max_reattempts = models.SmallIntegerField(default=3)
    _terminating = models.BooleanField(default=False,help_text='this workflow is terminating')
    
    def __init__(self, *args, **kwargs):
        super(Workflow,self).__init__(*args, **kwargs)
        validate_name(self.name)
        #Validate unique name
        if Workflow.objects.filter(name=self.name).exclude(pk=self.id).count() >0:
            raise ValidationError('Workflow with name {0} already exists.  Please choose a different one or use .resume()'.format(self.name))
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
        variable will probably hit a cache, so this function forces a database query.
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
    def resume(name=None,dry_run=False):
        """
        Resumes a workflow from the last failed node.  Automatically deletes any
        unsuccessful nodes and their associated files, so if you try to
        `add_node()` a node with a name that already exists but failed,
        you can change its parameters like pre_command and outputs.
        
        :param name: A unique name for this workflow
        :param dry_run: This workflow is a dry run.  No jobs will actually be executed
        :param root_output_dir: Optional override of the root_output_dir contains in the configuration file
        """
        name = re.sub("\s","_",name)
        if Workflow.objects.filter(name=name).count() == 0:
            print >> sys.stderr,'Workflow {0} does not exist, cannot resume it'.format(name)
            return Workflow.create(name=name)
        wf = Workflow.objects.get(name=name)
        wf._terminating=False
        wf.resume_from_last_failure=True
        wf.dry_run=dry_run
        wf.save()
        wf.log.info('Resuming this workflow.')
        Batch.objects.filter(workflow=wf).update(order_in_workflow=None)
        if dry_run:
            wf.log.info('Doing a Dry Run.')
        return wf

    @staticmethod
    def restart(name=None,root_output_dir=None,dry_run=False):
        """
        Restarts a workflow.  Will delete the old workflow and all of its files
        but will retain the old workflow id for convenience
        :param name: A unique name for this workflow
        :param dry_run: This workflow is a dry run.  No jobs will actually be executed
        :param root_output_dir: Optional override of the root_output_dir contains in the configuration file
        """
        name = re.sub("\s","_",name)
        old_wf_exists = Workflow.objects.filter(name=name).count() > 0
        
        if root_output_dir is None:
            root_output_dir = cosmos_settings.default_root_output_dir
            
        if old_wf_exists:
            old_wf = Workflow.objects.get(name=name)
            wf_id = old_wf.id
            old_wf.delete(delete_files=True)
        else:
            wf_id=None
        
        new_wf = Workflow.create(_wf_id=wf_id,name=name,root_output_dir=root_output_dir,dry_run=dry_run)
        if not old_wf_exists:
            new_wf.log.warning('Tried to restart a workflow that doesn\'t exist yet'.format(name))
        new_wf.log.info('Restarting this Workflow.')
        
        return new_wf
                
    @staticmethod
    def create(name=None,dry_run=False,root_output_dir=None,_wf_id=None):
        """
        Creates a new workflow
        :param name: A unique name for this workflow
        :param dry_run: This workflow is a dry run.  No jobs will actually be executed
        :param root_output_dir: Optional override of the root_output_dir contains in the configuration file
        """
        name = re.sub("\s","_",name)
        if name is None:
            raise ValidationError('Name of a workflow cannot be None')
        #set default output_dir?
        if root_output_dir is None:
            root_output_dir = cosmos_settings.default_root_output_dir
        
        check_and_create_output_dir(root_output_dir)
        output_dir = os.path.join(root_output_dir,name)
            
        wf = Workflow.objects.create(id=_wf_id,name=name,
                            output_dir=output_dir,
                            dry_run=dry_run)
        wf.save()
        wf.log.info('Created Workflow {0}.'.format(wf))
        if dry_run:
            wf.log.info('Doing a Dry Run.')
        return wf
            
        
    def add_batch(self,name, hard_reset=False):
        """
        Adds a batch to this workflow.
        :parameter name: The name of the batch, must be unique within this Workflow
        :parameter hard_reset: Delete any batch with this name including all of its nodes
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
        
        batch_exists = Batch.objects.filter(workflow=self,name=name)
        
        if hard_reset:
            if not batch_exists:
                self.log.warning("Batch does not exist so there is no hard_reset to perform.")
            else:
                self.log.info("Doing a hard reset on batch {0}.".format(name))
                Batch.objects.get(workflow=self,name=name).delete()
                
        b, created = Batch.objects.get_or_create(workflow=self,name=name)
        if created:
            self.log.info('Creating {0} from scratch.'.format(b))
        else:
            self.log.info('{0} already exists, and resume is on so loading it from history...'.format(b))
            
        b.order_in_workflow = order_in_workflow
        b.save()
        return b
        
 
    def get_resource_usage(self):
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
        d = os.path.join(self.output_dir,'_plots')
        with file(d,'wb') as f:
            for line in self.get_resource_usage():
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
                                     drmaa_native_specification=get_rusage(cosmos_settings.DRM,node.memory_requirement))
        node._jobAttempts.add(jobAttempt)
        if self.dry_run:
            self.log.info('Dry Run: skipping submission of job {0}'.format(jobAttempt))
        else:
            self.jobManager.submitJob(jobAttempt)
            self.log.info('Submitted jobAttempt with drmaa jobid {0}'.format(jobAttempt.drmaa_jobID))
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
    
    def __wait(self,batch=None,stop_on_fail=False):
        """
        Waits for all executing nodes to finish.  Returns an array of the nodes that finished.
        if batch is omitted or set to None, all running nodes will be waited on
        """
        waiting_on_workflow = self
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
                    if self._is_terminating() and stop_on_fail:
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


    def wait(self, batch=None, stop_on_fail=True):
        """
        Waits for all executing nodes to finish.  Returns an array of the nodes that finished.
        if batch is omitted or set to None, all running nodes will be waited on
        """
        nodes = self.__wait(batch=batch,stop_on_fail=stop_on_fail)
        self._check_termination(check_for_leftovers=True)
        return nodes

    def run_wait(self,batch):
        """
        shortcut to run_batch(); wait(batch=batch);
        """
        self.run_batch(batch=batch)
        return self.wait(batch=batch)

    def finished(self):
        """
        call at the end of every workflow.
        if there any left over jobs that have not been collected,
        it will wait for all of them them
        will also run _clean_up()
        """
        self._check_and_wait_for_leftover_nodes()
        self._clean_up()
        self.log.info('Finished.')
        
    def _check_and_wait_for_leftover_nodes(self):
        """Checks and waits for any leftover nodes"""
        if self.nodes.filter(status='in_progress').count()>0:
            self.log.warning("There are left over nodes in the queue, waiting for them to finish")
            self.__wait()
        
    def _clean_up(self):
        """
        Should be executed at the end of a workflow.
        """
        self.log.debug("Cleaning up workflow")
        Batch.objects.filter(workflow=self,order_in_workflow=None).delete() #these batches weren't used again after a restart
               
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
    
    
    def get_tagged_nodes(self,batch=None,_op="and",**kwargs):
        """
        returns nodes that are tagged with any set of key/vaue pairs
        :param batch: optionally pass a batch in to search only that batch
        :param _op: choose to either "and" or "or" the key/values together when searching for nodes
        usage: workflow.get_tagged_nodes(batch=my_batch,shape="circle",color="orange")
        """
        
        if len(kwargs) == 0: #no tags
            if batch: return batch.nodes
            return self.nodes
            
        if batch is None:
            nodeTag_matches = NodeTag.objects.filter(node__batch__workflow=self)
        else:
            nodeTag_matches = NodeTag.objects.filter(node__batch=batch)
            

        Q_list = []
        for key,val in kwargs.items():
            Q_list.append(Q(key=key,value=val))
        if _op == 'and':
            Qs = reduce(lambda x,y: x & y,Q_list)
        else:
            Qs = reduce(lambda x,y: x | y,Q_list)
             
        return Node.objects.filter(nodetag__in=nodeTag_matches.filter(Qs))


    
class Batch(models.Model):
    """
    Executes a list of commands via drmaa.
    Should not be directly created.  Use workflow.addBatch() to create a new batch.
    """
    name = models.CharField(max_length=200)
    workflow = models.ForeignKey(Workflow)
    order_in_workflow = models.IntegerField(null=True)
    status = models.CharField(max_length=200,choices=status_choices,default='no_attempt') 
    successful = models.BooleanField(default=False)
    
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
        m = JobAttempt.objects.filter(node_set__in = Node.objects.filter(batch=self)).aggregate(models.Max('drmaa_utime'))['drmaa_utime__max']
        if m is None:
            return None
        return m
    
    @property
    def total_job_time(self):
        t = JobAttempt.objects.filter(node_set__in = Node.objects.filter(batch=self)).aggregate(models.Sum('drmaa_utime'))['drmaa_utime__sum']
        if t is None:
            return None
        return t
    
    @property
    def file_size(self):
        return folder_size(self.output_dir)
    
    @property
    def output_dir(self):
        return os.path.join(self.workflow.output_dir,self.name)
    
    @property
    def nodes(self):
        return Node.objects.filter(batch=self)
    
    def numNodes(self):
        return Node.objects.filter(batch=self).count()
    
    def add_node(self, name, pcmd, outputs={}, hard_reset=False, mem_req = 0, tags = {}):
        """
        Adds a node to the batch.
        :param pre_cmd: (str) The preformatted command to execute
        :param outputs: (dict) a dictionary of outputs and their names
        :param hard_reset: (bool) Deletes this node and all associated files and start it fresh
        :param mem_req: (int) Optional setting to tell the DRM how much memory to reserve in MB
        :param **kwargs: any other keyword arguments will add `tag`s to the node
        """
        
        #validation
        if name == '' or name is None:
            raise ValidationError('name cannot be blank')
        if pcmd == '' or pcmd is None:
            raise ValidationError('pre_command cannot be blank')
        
        name = re.sub("\s","_",name) #user convenience
        pre_command = pcmd  #user convenience
        
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
               
        node,created = Node.objects.get_or_create(batch=self,name=name,defaults={'pre_command':pcmd,'outputs':outputs,'memory_requirement':mem_req})
        
        #validation
        if (not created) and node.successful:  
            if node.pre_command != pcmd:
                self.log.error("You can't change the pcmd of a existing successful node (keeping the one from history).  Use hard_reset=True if you really want to do this.")
            if node.outputs != outputs:
                self.log.error("You can't change the outputs of an existing successful node (keeping the one from history).  Use hard_reset=True if you really want to do this")
        
        if created:
            self.log.info("Created node {0} from scratch".format(node))
            #if adding to a finished batch, set that batch's status to in_progress so new nodes are executed
            batch = node.batch
            if batch.is_done():
                batch.status = 'in_progress'
                batch.successful = False
                batch.save()
        
        #this error should never occur    
        elif not created and not self.successful:
            if self.workflow.resume_from_last_failure and node.successful:
                self.log.error("Loaded successful node {0} in unsuccessful batch {0} from history".format(node,node.batch))

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
            self.save()
            self.log.info('Batch {0} successful!'.format(self))
        elif num_nodes_failed + num_nodes_successful == num_nodes:
            self.status='failed'
            self.save()
            self.log.warning('Batch {0} failed!'.format(self))
    
    def get_tagged_nodes(self,_op='and',**kwargs):
        """
        Get nodes by keyword
        """
        return self.workflow.get_tagged_nodes(batch=self, _op=_op, **kwargs)
                
    def group_nodes(self,*args):
        """
        Yields nodes, grouped by tags in *args.
        :returns: a tuple of (tags used,nodes in group)
        note: nodes without a single tag in *args will be ignored
        usage: group_nodes('color','shape') will yield a tuple of the tags used, and the groups of nodes with different colors and shapes,
               for example ({'color':'red','shape':'circle'},[node1,node2,node3])
        """
        node_tag_values = NodeTag.objects.filter(node__in=self.nodes, key__in=args).values() #get this batch's tags
        
        itr = helpers.groupby(node_tag_values,lambda x: x['node_id'])
        node_ids2kvps = dict( [ (node_id,set([ (ntv['key'],ntv['value']) for ntv in ntvs ])) for node_id,ntvs in itr ] ) #ntvs = node_tag_values, #kvp = key_value_pair
        # node_ids2kvps is a dict who's keys are node_ids and values are a set of (key,val) tags)
        node_ids = node_ids2kvps.keys()    
            
        for kvps,node_ids in helpers.groupby(node_ids,lambda x: node_ids2kvps[x]): #this works because the key value pairs are unique sets
            node_ids = [ x for x in node_ids ]
            if len(kvps) == len(args): #make sure all kvps were used.  this could be more efficient, but oh well
                yield dict(kvps),Node.objects.filter(pk__in=node_ids)
    
    def delete(self, *args, **kwargs):
        """
        Deletes this batch and all files associated with it
        """
        self.log.debug('Deleting Batch {0}'.format(self.name))
        self.nodes.all().delete()
        if os.path.exists(self.output_dir):
            os.system('rm -rf {0}'.format(self.output_dir))
        super(Batch, self).delete(*args, **kwargs)
    
    @models.permalink    
    def url(self):
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
    node = models.ForeignKey('Node')
    key = models.CharField(max_length=63)
    value = models.CharField(max_length=255)
    
    def __str__(self):
        return "Tag({0}) - {0}: {0}".format(self.node, self.key, self.value)

class Node(models.Model):
    
    _jobAttempts = models.ManyToManyField(JobAttempt,related_name='node_set')
    pre_command = models.TextField(help_text='preformatted command.  almost always will contain the special string {output} which will later be replaced by the proper output path')
    exec_command = models.TextField(help_text='the actual command that was executed',null=True)
    name = models.CharField(max_length=255,null=True)
    memory_requirement = models.IntegerField(help_text="Minimum Memory Requirement in MB",default=None,null=True)
    batch = models.ForeignKey(Batch,null=True)
    successful = models.BooleanField(null=False)
    status = models.CharField(max_length=100,choices = status_choices,default='no_attempt')
    outputs = PickledObjectField(null=True) #dictionary of outputs
    
    def __init__(self, *args, **kwargs):
        """Init Node"""
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
        return self.batch.workflow
    
    @property
    def node_tags(self):
        return NodeTags.objects.filter(node=self)

    @property
    def log(self):
        """This node's workflow's log"""
        return self.workflow.log

    @property
    def file_size(self,human_readable=True):
        """Node filesize"""
        return folder_size(self.output_dir,human_readable=human_readable)
    
    @property
    def output_dir(self):
        """Node output dir"""
        return os.path.join(self.batch.output_dir,self.name)
    
    @property
    def job_output_dir(self):
        """Where the job output goes"""
        return os.path.join(self.output_dir,'out')
    
    @property
    def output_paths(self):
        """Dictionary of outputs and their full absolute paths"""
        r = {}
        for key,val in self.outputs.items():
            r[key] = os.path.join(self.job_output_dir,val)
        return r
    
    @property
    def jobAttempts(self):
        return self._jobAttempts.all().order_by('id')
    
    @property
    def time_to_run(self):
        return self.get_successful_jobAttempt().drmaa_utime if self.successful else None
    
    def get_numAttempts(self):
        return self._jobAttempts.count()
    
    def get_successful_jobAttempt(self):
        """
        Return this node's successful job attempt.  If there were no successful job attempts
        return None
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
        Executed whenever this node finishes by the workflow
        """
        if self._jobAttempts.filter(successful=True).count():
            self.status = 'successful'
            self.successful = True
            self.log.info("{0} Successful!".format(self,jobAttempt))
            self.save()
        else:
            self.status = 'failed'
            self.log.info("{0} Failed!".format(self,jobAttempt))
            self.save()
        
    def tag(self,**kwargs):
        """
        tag this node with keys and values.  keys must be unique
        usage: node.tag(color="blue",shape="circle")
        """
        #TODO don't allow tags called things like 'status' or other node attributes
        for key,value in kwargs.items():
            value = str(value)
            nodetag, created = NodeTag.objects.get_or_create(node=self,key=key,defaults= {'value':value})
            nodetag.save()

    @property            
    def tags(self):
        """
        Returns the dictionary of this node's tags
        """
        return dict([(x['key'],x['value']) for x in self.nodetag_set.all().values('key','value')])
    
    @models.permalink    
    def url(self):
        return ('node_view',[str(self.id)])


    def delete(self, *args, **kwargs):
        """
        Deletes this node and all files associated with it
        """
        self.log.info('Deleting node {0} and its output directory {0}'.format(self.name,self.output_dir))
        self._jobAttempts.all().delete()
        self.node_tags.delete()
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
                                attempts=self.get_numAttempts(),
                                successful=self.successful,
                                drmaa_stdout=drmaa_stdout,
                                drmaa_stderr=drmaa_stderr)



