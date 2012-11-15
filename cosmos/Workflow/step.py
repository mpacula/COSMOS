from Cosmos.helpers import parse_cmd

workflow = None
settings = {}

def merge_dicts(x,y):
    for k,v in y.items(): x[k]=v
    return x

def dict2task_name(d):
    s = ' '.join([ '{0}-{1}'.format(k,v) for k,v in d.items() ])
    if s == '': return 'task'
    return s

class StepError(Exception):
    pass

def validate_dict_has_keys(d,keys):
    """makes sure keys are defined in dict"""
    for k in keys:
        if not k in d:
            raise StepError('The dictionary returned does not have the required keyword {0} defined'.format(k))

class Step():
    outputs = {}
    mem_req = 0
    cpu_req = 1
    
    def __init__(self,name=None,hard_reset=False,**kwargs):
        """
        :param hard_reset: Deletes the old stage before creating this one  
        """
        if workflow is None:
            raise Exception('Set the parameter step.workflow to your Workflow before adding steps.')
        
        if name is None:
            name = type(self)
        self.name = name 
        
        self.stage = workflow.add_stage(self.name,hard_reset=hard_reset)


    def _parse_cmd2(self,string,dictnry,**kwargs):
        'shortcut to combine with dict with kwargs and extra_parse_cmd_dict'
        #TODO throw an error if there are key conflicts
        d = merge_dicts(kwargs,dictnry)
        d['settings'] = settings
        d['self'] = self
        return parse_cmd(string,**d)
    
    def one2one(self,input_stage,*args,**kwargs):
        if not self.stage.successful:
            for n in input_stage.tasks:
                r = self.one2one_cmd(input_task=n,*args,**kwargs)
                pcmd_dict = r.setdefault('pcmd_dict',{})
                self.stage.add_task(name = n.name,
                                    pcmd = self._parse_cmd2(r['pcmd'],pcmd_dict,input_task=n,tags=n.tags),
                                    tags = n.tags,
                                    outputs = self.outputs,
                                    mem_req = self.mem_req,
                                    cpu_req = self.cpu_req)
            workflow.run_wait(self.stage)
        return self.stage
    
    def many2one(self,input_stage,group_by=[],*args,**kwargs):
        """
        
        :param group_by: a list of tag keywords with which to parallelize input by.  see the keys parameter in :func:`Workflow.models.Workflow.group_tasks_by`.  An empty list will simply place all tasks in the stage into one group.
        
        """
        #TODO make sure there are no name conflicts in kwargs and 'input_stage' and 'group_by'
        if not self.stage.successful:
            for tags,input_tasks in input_stage.group_tasks_by(keys=group_by):
                r = self.many2one_cmd(input_tasks=input_tasks,tags=tags,*args,**kwargs)
                pcmd_dict = r.setdefault('pcmd_dict',{})
                self.stage.add_task(name = dict2task_name(tags),
                                    pcmd = self._parse_cmd2(r['pcmd'],pcmd_dict,tags=tags),
                                    tags = tags,
                                    outputs = self.outputs,
                                    mem_req = self.mem_req,
                                    cpu_req = self.cpu_req)
            workflow.run_wait(self.stage)
        return self.stage
    
    def one2many(self,input_stage,*args,**kwargs):
        """
        Used when one input task becomes multiple output tasks
        """
        if not self.stage.successful:
            for n in input_stage.tasks:
                for r in self.one2many_cmd(input_task=n,*args,**kwargs):
                    validate_dict_has_keys(r,['pcmd','add_tags'])
                    pcmd_dict = r.setdefault('pcmd_dict',{})
                    new_tags = merge_dicts(n.tags, r['add_tags'])
                    self.stage.add_task(name = dict2task_name(new_tags),
                                        pcmd = self._parse_cmd2(r['pcmd'],pcmd_dict,input_task=n,tags=n.tags),
                                        tags = new_tags,
                                        outputs = self.outputs,
                                        mem_req = self.mem_req,
                                        cpu_req = self.cpu_req)
            workflow.run_wait(self.stage)
        return self.stage
    
    def many2many(self,input_stage,*args,**kwargs):
        """
        Used when the parallelization is complex enough that the command should specify it.  The func:`self.many2many_cmd` will be passed
        the entire input_stage rather than any tasks.
        """
        if not self.stage.successful:
            for r in self.many2many_cmd(input_stage=input_stage,*args,**kwargs):
                #Set defaults
                validate_dict_has_keys(r,['pcmd','new_tags'])
                pcmd_dict = r.setdefault('pcmd_dict',{})
                name = r.setdefault('name',dict2task_name(r['new_tags']))
                self.stage.add_task(name = name,
                                    pcmd = self._parse_cmd2(r['pcmd'],pcmd_dict,input_stage=input_stage,tags=r['new_tags']),
                                    tags = r['new_tags'],
                                    outputs = self.outputs,
                                    mem_req = self.mem_req,
                                    cpu_req = self.cpu_req)
            workflow.run_wait(self.stage)
        return self.stage
    
    
    def one2one_cmd(self,input_task,*args,**kwargs):
        """
        The command to run
        
        :returns: {pcmd, pcmd_format_dictionary}.  pcmd is required.
        """
        raise NotImplementedError()
    
    def many2one_cmd(self,input_tasks,tags,*args,**kwargs):
        """"
        The command to run
        
        :returns: {pcmd, pcmd_format_dictionary}.  pcmd is required.
        """
        raise NotImplementedError()
    
    def one2many_cmd(self,input_task,*args,**kwargs):
        """
        The command to run
        
        :yields: {pcmd, pcmd_format_dictionary, add_tags}.  pcmd, add_tags is required.
        """
        raise NotImplementedError()
    
    def many2many_cmd(self,input_stage,*args,**kwargs):
        """
        The command to run
        
        :yields: {pcmd, pcmd_format_dictionary, new_tags, name}.  pcmd and new_tags is required.
        """
        raise NotImplementedError()
    
    
    
    
    
    