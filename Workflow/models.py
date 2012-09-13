from django.db import models
from django.db.models import Q
from JobManager.models import JobAttempt,JobManager
import logging
import datetime
import os
from Cosmos.helpers import validate_name,validate_not_null, check_and_create_output_dir, folder_size, get_workflow_logger
import stat
from django.core.exceptions import ValidationError
from picklefield.fields import PickledObjectField
import shutil
from Cosmos import cosmos

from django.dispatch import receiver
    
status_choices=(
                ('successful','Executed successfully.'),
                ('no_attempt','No attempt has been made to execute this node.'),
                ('in_progress','Node execution in progress. It\'s either waiting in the Queue or being Executed'),
                ('failed','Execution has been attempted, but failed.')
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
    
    def terminate(self):
        """
        terminates this workflow
        """
        self.log.warning("Terminating this workflow...")
        self._terminating = True
        self.save()
        self.jobManager.terminate_all_queued_or_running_jobAttempts()
        self.log.warning("Terminated.")
        
    
    @property
    def _batches(self):
        return Batch.objects.filter(workflow=self)
    
    @property
    def batches(self):
        return Batch.objects.filter(workflow=self).all().order_by('order_in_workflow')
    
    @property
    def file_size(self):
        return folder_size(self.output_dir)
    
    
    def __init__(self, *args, **kwargs):
        super(Workflow,self).__init__(*args, **kwargs)
        if self.id is None: #creating for the first time
            #validate unique name
            name = kwargs['name']
            if Workflow.objects.filter(name=name).count() >0:
                raise ValidationError('Workflow with name {} already exists.  Please choose a different one or use .resume()'.format(name))
            #create jobmanager
            if self.jobManager == None:
                self.jobManager = JobManager.objects.create()
                self.jobManager.save()
            
        #validate
        validate_name(self.name)
        check_and_create_output_dir(self.output_dir)
        
        #logger
        self.log, self.log_path = get_workflow_logger(self)
        
    @property
    def log_txt(self):
        return file(self.log_path,'rb').read()
    
    @staticmethod
    def resume(name=None):
        """
        Resumes a workflow from the last failed node.  Automatically deletes any
        unsuccessful nodes and their associated files, so if you try to
        `add_node()` a node with a name that already exists but failed,
        you can change its parameters like pre_command and outputs.
        """
        if Workflow.objects.filter(name=name).count() == 0:
            raise ValidationError('Workflow {} does not exist, cannot resume it'.format(name))
        wf = Workflow.objects.get(name=name)
        wf._terminating=False
        wf.resume_from_last_failure=True
        wf.save()
        wf.log.info('Resuming this workflow.')
        Batch.objects.filter(workflow=wf).update(order_in_workflow=None)
        return wf

    @staticmethod
    def restart(name=None,root_output_dir=None,):
        """
        
        """
        if Workflow.objects.filter(name=name).count() == 0:
            log.warning('Tried to restart a workflow that doesn\'t exists'.format(name))
            wf_id = None
        else:
            wf = Workflow.objects.get(name=name)
            wf_id = wf.id
            wf.delete(delete_files=True)
        
        new_wf = Workflow.create(_wf_id=wf_id,name=name,root_output_dir=root_output_dir)
        new_wf.log.info('Restarting this Workflow.')
        return new_wf
                
    @staticmethod
    def create(name=None,dry_run=False,root_output_dir=None,_wf_id=None):
        if name is None:
            raise ValidationError('Name of a workflow cannot be None')
        #set default output_dir?
        if root_output_dir is None:
            root_output_dir = cosmos.default_root_output_dir
        
        check_and_create_output_dir(root_output_dir)
        output_dir = os.path.join(root_output_dir,name)
            
        wf = Workflow.objects.create(id=_wf_id,name=name,
                            output_dir=output_dir,
                            dry_run=dry_run)
        wf.save()
        wf.log.info('Created this Workflow.')
        return wf
            
        
    def close_session(self):
        if hasattr(self.jobManager,'session'):
            self.log.info('Ending session')
            self.jobManager.close_session()
        
    def add_batch(self,name, hard_reset=False):
        """
        Adds a batch to this workflow.
        :parameter name: The name of the batch, must be unique within this Workflow
        :parameter hard_reset: Delete any batch with this name including all of its nodes
        """
        self.log.info("Adding batch {}".format(name))
        #determine order
        m = Batch.objects.filter(workflow=self).aggregate(models.Max('order_in_workflow'))['order_in_workflow__max']
        if m is None:
            order_in_workflow = 1
        else:
            order_in_workflow = m+1
        
        batch_exists = Batch.objects.filter(workflow=self,name=name)
        
        if hard_reset:
            if not batch_exists:
                raise ValidationError("Batch does not exist.  Cannot proceed with a hard_reset.  Please set hard_reset to True in order to continue.")
            else:
                log.info("Doing a hard reset on batch {}".format(name))
                Batch.objects.get(workflow=self,name=name).delete()
                
        if self.resume_from_last_failure:
            b, created = Batch.objects.get_or_create(workflow=self,name=name)
            if created:
                self.log.info('Creating {} from scratch'.format(b))
            else:
                self.log.info('Node {} already exists, and resume is on so loading it from history.'.format(b))
        else:
            b = Batch.objects.create(workflow=self,name=name)
        b.order_in_workflow = order_in_workflow
        b.save()
        return b
        
    def delete(self, *args, **kwargs):
        self.close_session()
        self.jobManager.delete()
        self.save()
        
        delete_files=False
        if 'delete_files' in kwargs:
            delete_files = kwargs.pop('delete_files')
        
        super(Workflow, self).delete(*args, **kwargs)
        
        #delete_files=True will delete all output files
        if delete_files:
            self.log.info('deleting directory {}'.format(self.output_dir))
            if os.path.exists(self.output_dir):
                shutil.rmtree(self.output_dir)
                
        
    def __create_command_sh(self,command,file_path):
        with open(file_path,'wb') as f:
            f.write('#!/bin/sh\n')
            f.write(command)
        os.system('chmod 700 {}'.format(file_path))

    def _runNode(self,node):
        """
        Executes a node and returns a jobAttempt
        """
        node.batch.status = 'in_progress'
        node.batch.save()
        node.status = 'in_progress'
        self.log.info('Running node {0}'.format(node))
        check_and_create_output_dir(node.output_dir)
        try:
            node.exec_command = node.pre_command.format(output_dir=node.output_dir,outputs = node.outputs)
        except KeyError:
            self.log.error('Failed to format() pre_command:\n{}\nwith:\noutput_dir={} and outputs = {}'.format(node.pre_command,node.output_dir,node.outputs))
            raise ValidationError('Command for node {} is formatted incorrectly'.format(node))
                
        #create command.sh that gets executed
        command_script_path = os.path.join(node.output_dir,'command.sh')
        self.__create_command_sh(node.exec_command,command_script_path)
        
        jobAttempt = self.jobManager.addJobAttempt(command_script_path=command_script_path,
                                     drmaa_output_dir=os.path.join(node.output_dir,'drmaa_out/'),
                                     jobName=node.name,
                                     drmaa_native_specification=''.format(node.memory_required))
#                                     drmaa_native_specification='-shell yes -b yes -l h_vmem={0},virtual_free={0}'.format(node.memory_required))
        node._jobAttempts.add(jobAttempt)
        if self.dry_run:
            self.log.info('Dry Run: skipping submission of job {0}'.format(jobAttempt))
        else:
            self.jobManager.submitJob(jobAttempt)
            self.log.info('submitted jobAttempt with jobAttempt id {0}'.format(jobAttempt.drmaa_jobID))
        node.save()
        self.jobManager.save()
        return jobAttempt

    def run_and_wait(self,batch):
        """
        shortcut to run_batch(); wait_on_batch();
        """
        pass

#will have to implement with my own wait loop
#    def synch_batch(self,batch):
#        self.log.info('waiting on batch {}'.format(batch))
#        self.jobManager.drmaa_session.

    def run_batch(self,batch):
        self.log.info('Running batch {}'.format(batch))
        if batch.successful:
            self.log.info('{0} has already been executed successfully, skip run.'.format(batch))
            return
        for node in batch.nodes:
            if node.successful:
                self.log.info('{0} has already been executed successfully, skip run.'.format(node))
            else:
                self.log.info('{0} has not been executed successfully.'.format(node))
                self._runNode(node)
                
    def wait_on_all_nodes(self):
        """
        Waits for all executing nodes to finish.  Returns an array of the nodes that finished.
        """
        nodes = []
        self.log.info('Waiting on all nodes...')
        for jobAttempt in self.jobManager.yield_All_Queued_Jobs():
            jobAttempt_node = jobAttempt.node
            self.log.info('Finished jobAttempt for node {0}'.format(jobAttempt_node))
            nodes.append(jobAttempt_node)
            jobAttempt_node._jobAttempt_done_receiver(jobAttempt)
        self.log.info('All nodes for this wait have completed.')
        if self._terminating:
            self.log.warning("Termination complete, exiting with exit code 2")
            self._terminating=False
            self.save()
            import sys; sys.exit(2)
        return nodes 
            
    def wait_on_batch(self,batch):
        raise NotImplemented()        
            

    def clean_up(self):
        """
        executed ONLY at the end of a workflow.  right now just removes old batches that weren't used in last resume
        """
        self.log.info("Cleaning up workflow")
        Batch.objects.filter(workflow=self,order_in_workflow=None).delete()
    
    def restart_from_here(self):
        """
        Deletes any batches in the history that haven't been added yet
        """
        self.log.info('Restarting Workflow from here.')
        Batch.objects.filter(workflow=self,order_in_workflow=None).delete()
    
    def __str__(self):
        return 'Workflow[{}] {}'.format(self.id,self.name)
            
    def toString(self):
        s = 'Workflow[{0.id}] {0.name} resume_from_last_failure={0.resume_from_last_failure}\n'.format(self)
        #s = '{:*^72}\n'.format(s) #center s with stars around it
        for batch in self.batches:
            s = s + batch.toString(tabs=1)
        return s
    
    @models.permalink    
    def url(self):
        return ('workflow_view',[str(self.id)])


    
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
        if self.workflow._batches.filter(name=self.name).exclude(id=self.id).count() > 0:
            raise ValidationError("Batch names must be unique within a given Workflow. The name {} already exists.".format(self.name))

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
    def max_time_to_run(self):
        m = JobAttempt.objects.filter(node_set__in = Node.objects.filter(batch=self)).aggregate(models.Max('drmaa_utime'))['drmaa_utime__max']
        if m is None:
            return None
        return m
    
    @property
    def total_time_to_run(self):
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
        return Node.objects.filter(batch=self).all()
    
    def numNodes(self):
        return Node.objects.filter(batch=self).count()
    
    def add_node(self, name, pre_command, outputs, hard_reset=False):
        """
        Adds a node to the batch.
        :param pre_command: The preformatted command to execute
        :param outputs: a dictionary of outputs and their names
        :param hard_reset: Deletes this node and all associated files and start it fresh
        """
        node_exists = Node.objects.filter(batch=self,name=name).count() > 0
        if node_exists:
            node = Node.objects.get(batch=self,name=name)
        
        #validation
        if name == '' or name is None:
            raise ValidationError('name cannot be blank')
        if pre_command == '' or pre_command is None:
            raise ValidationError('pre_command cannot be blank')
        
        #delete if hard_reset
        if hard_reset:
            if not node_exists:
                raise ValidationError("Cannot hard_reset node with name {} as it doesn't exist.".format(name))
            node.delete()
        
        if node_exists and (not node.successful) and self.workflow.resume_from_last_failure:
            self.log.info("Node was unsuccessful last time, trying again {}".format(node))
            node.delete()
        
        #if not self.workflow.resume_from_last_failure and node_exists:          
        #    raise ValidationError("Node with name {} already exists and not resuming.  Either resume the workflow or hard_reset this node.")
        
        node,created = Node.objects.get_or_create(batch=self,name=name,defaults={'pre_command':pre_command,'outputs':outputs})
        
        #validation
        if (not created) and node.successful and (not node_exists):  
            if node.pre_command != pre_command:
                raise ValidationError("You can't change the pre_command of a existing successful node.  Use hard_reset=True if you really want to do this")
            if node.outputs != outputs:
                raise ValidationError("You can't change the outputs of an existing successful node.  Use hard_reset=True if you really want to do this")
        if created:
            self.log.info("Created node {} from scratch".format(node))
        elif not created and not self.successful:
            if self.workflow.resume_from_last_failure and node.successful:
                self.log.warning("Loaded successful node {} in unsuccessful batch {} from history".format(node,node.batch))
                
        node.save()
        return node
        
    def _nodeAttempt_done_receiver(self,node):
        num_nodes = Node.objects.filter(batch=self).count()
        num_nodes_successful = Node.objects.filter(batch=self,successful=True).count()
        num_nodes_failed = Node.objects.filter(batch=self,status='failed').count()
        if num_nodes_successful == num_nodes:
            self.successful = True
            self.status = 'successful'
            self.save()
            log.info('Batch {} successful!'.format(self))
        elif num_nodes_failed + num_nodes_successful == num_nodes:
            self.status='failed'
            self.save()
            log.info('Batch {} failed!'.format(self))
                
    
          
    def delete(self, *args, **kwargs):
        self.log.debug('Deleting Batch {0}'.format(self.name))
        self.nodes.all().delete()
        super(Batch, self).delete(*args, **kwargs)
    
    @models.permalink    
    def url(self):
        return ('batch_view',[str(self.id)])
        
    def __str__(self):
        return 'Batch[{1}] {0}'.format(self.name,self.id,)
    
    def toString(self,tabs=0):
        # s = ['-'*72]
        s = ['{tabs}Batch[{1}] {0}'.format(self.name,self.id,tabs="  "*tabs)]
        #s = ['-'*72]
        for node in self.nodes.all():
            node_str = '  '*(tabs+1)+node.toString()
            node_str = node_str.replace("\n","{0}{1}".format('\n',"  "*(tabs+2)))
            s.append(node_str)
        return '\n'.join(s)
            

class Node(models.Model):
    
    _jobAttempts = models.ManyToManyField(JobAttempt,related_name='node_set')
    pre_command = models.TextField(help_text='preformatted command.  almost always will contain the special string {output} which will later be replaced by the proper output path')
    exec_command = models.TextField(help_text='the actual command that was executed',null=True)
    name = models.CharField(max_length=255,null=True)
    memory_required = models.CharField(max_length=200,help_text="ex: 10G",default="5G")
    batch = models.ForeignKey(Batch,null=True)
    successful = models.BooleanField(null=False)
    status = models.CharField(max_length=100,choices = status_choices,default='no_attempt')
    outputs = PickledObjectField(null=True) #dictionary of outputs   
    
    @property
    def workflow(self):
        return self.batch.workflow

    @property
    def log(self):
        return self.workflow.log

    @property
    def file_size(self):
        return folder_size(self.output_dir)
    
    @property
    def outputs_fullpaths(self):
        r = {}
        for key,val in self.outputs.items():
            r[key] = os.path.join(self.output_dir,val)
        return r
    
    def __init__(self, *args, **kwargs):
        super(Node,self).__init__(*args, **kwargs)
        validate_name(self.name)
        if Node.objects.filter(batch=self,name=self.name).exclude(id=self.id).count() > 0:
            raise ValidationError("Nodes belonging to a batch with the same name detected!".format(self.name))
        if self.id is None: #creating for the first time
            check_and_create_output_dir(self.output_dir) 
        
    @property
    def output_dir(self):
        return os.path.join(self.batch.output_dir,self.name)
    
    @property
    def jobAttempts(self):
        return self._jobAttempts.all().order_by('id')
    
    @property
    def time_to_run(self):
        return self.get_successful_job().drmaa_utime if self.successful else None
    
    def get_numAttempts(self):
        return self._jobAttempts.count()
    
    def get_successful_job(self):
        jobs = self._jobAttempts.filter(successful=True)
        if len(jobs) == 1:
            return jobs[0]
        elif len(jobs) > 1:
            raise Exception('more than 1 successful job, something went wrong!')
        else:
            return None # no successful jobs
        
    def _hasFinished(self):
        """
        Executed whenever this node finishes
        """
        pass

    def _jobAttempt_done_receiver(self,jobAttempt):
        batch = self.batch
        numAttempts = self._jobAttempts.count()
        if not jobAttempt.successful and self.batch.workflow._terminating==False: #ReRun jobAttempt
            if numAttempts < self.workflow.max_reattempts:
                self.log.warning("JobAttempt {} of self {} failed, this is attempt # {}, so retrying".format(jobAttempt, self,numAttempts))
                self.workflow._runNode(jobAttempt.node)
            else:
                self.log.warning("Node {} has reached max_reattempts of {}.  This self has failed".format(self, self.workflow.max_reattempts))
                self.status = 'failed'
                self.save()
        else:
            if jobAttempt.successful:
                self.status = 'successful'
                self.successful = True
                self.save()    
        batch._nodeAttempt_done_receiver(self)

    def delete(self, *args, **kwargs):
        self.log.info('Deleting node {} and its output directory {}'.format(self.name,self.output_dir))
        self._jobAttempts.all().delete()
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        super(Node, self).delete(*args, **kwargs)
    
    def __str__(self):
        return 'Node[{1}] {0}'.format(self.name,self.id)
        
    def toString(self):
        drmaa_stdout = '' #default if job is unsuccessful
        drmaa_stderr = '' #default if job is unsuccessful
        if self.successful:
            j = self.get_successful_job()
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



        
    



