from cosmos.Cosmos.helpers import parse_cmd
from django.core.exceptions import ValidationError
import re

workflow = None
default_pcmd_dict = {}

def merge_dicts(x,y):
    """
    Merges two dictionaries.  On duplicate keys, y's dictionary takes precedence.
    """
    x = x.copy()
    for k,v in y.items(): x[k]=v
    return x

def merge_mdicts(*args):
    """
    Merges dictionaries in *args.  Keys in the right most dicts take precedence
    """
    return reduce(merge_dicts,args)


def _unnest(a_list):
    """
    unnests a list
    
    .. example::
    >>> _unnest([1,2,[3,4],[5]])
    [1,2,3,4,5]
    """
    return [ item for items in a_list for item in items ]

def dict2task_name(d):
    s = ' '.join([ '{0}-{1}'.format(k,v) for k,v in d.items() ])
    if s == '': return 'task'
    return s

class StepError(Exception):
    pass

def validate_dict_has_keys(d,keys):
    """makes sure keys are defined in dict
    also makes sure the length if the key's value is not 0
    """
    for k in keys:
        if not k in d:
            raise StepError('The dictionary returned does not have the required keyword {0} defined'.format(k))
        elif len(d[k]) == 0:
            raise StepError('{0} has a length of 0'.format(k))


class Step():
    outputs = {}
    mem_req = 0
    cpu_req = 1
    new_tasks = []
    kwargs = {}
    
    @property
    def log(self):
        return self.stage.workflow.log
    
    def __init__(self,name=None,**kwargs):
        if workflow is None:
            raise Exception('Set the parameter step.workflow to your Workflow before adding steps.')
        
        if name is None:
            name = type(self)
        self.name = name 
        
        self.skip_add_task_checks = workflow.stages.filter(name = re.sub("\s","_",name)).count() == 0
        self.stage = workflow.add_stage(self.name)


    def _parse_cmd2(self,string,dictnry,**kwargs):
        'shortcut to combine with dict with kwargs and extra_parse_cmd_dict'
        #TODO throw an error if there are key conflicts
        d = merge_mdicts(default_pcmd_dict,kwargs,dictnry)
        if self not in d: d['self'] = self
        return parse_cmd(string,**d)
    
    
    
    def __new_task(self,pcmd,pcmd_dict,tags,parents):
        """adds a task"""
        return self.stage.new_task(name = '',
                                   pcmd = self._parse_cmd2(pcmd,pcmd_dict,tags=tags),
                                    tags = tags,
                                    save = False,
                                    skip_checks = self.skip_add_task_checks,
                                    parents = parents,
                                    outputs = self.outputs,
                                    mem_req = self.mem_req,
                                    cpu_req = self.cpu_req)
      
    
    def add_many_tasks(self,G,tags,parents,extra_pcmd_dict):
        #if type(parents) != list: raise StepError('parents parameter must be a list.  parents is {0} and set to {1}'.format(type(parents),parents))
        for r in G:
            validate_dict_has_keys(r,['pcmd','add_tags'])
            new_task = self.__new_task(pcmd = r['pcmd'],
                                     pcmd_dict = merge_mdicts(self.kwargs,{'tags':tags},r['add_tags'],extra_pcmd_dict,r.setdefault('pcmd_dict',{})),
                                     tags = merge_dicts(tags,r['add_tags']),
                                     parents = parents)
            self.new_tasks.append(new_task)
    
    def add_one_task(self,r,tags,parents,extra_pcmd_dict):
        #if type(parents) != list: raise StepError('parents parameter must be a list.  parents type is {0} and set to {1}'.format(type(parents),parents))
        validate_dict_has_keys(r,['pcmd'])
        add_tags = r.setdefault('add_tags',{})
        new_task = self.__new_task(pcmd = r['pcmd'],
                             pcmd_dict = merge_mdicts(self.kwargs,{'tags':tags},extra_pcmd_dict,r.setdefault('pcmd_dict',{}),add_tags),
                             tags = merge_dicts(add_tags,tags),
                             parents = parents)
        self.new_tasks.append(new_task)
        
        
    def __x2x(self,input_steps, _input_type, _output_type,group_by=None,*args,**kwargs):
        """
        proxy for all algorithms
        
        :param _input_type: 'many' or 'one' or 'none'
        :param ouput_type: 'many' or 'one'
        """
        
        def make_IND(input_steps,tags):
            """
            creates input_task_dict
            Only searches using common keywords beyond the first step in input_steps
            """
            r = {
                 input_steps[0].__class__.__name__ : input_steps[0].stage.get_task_by(tags)
            }
            for input_step in input_steps[1:]:
                tags2 = tags.copy()
                for k in tags:
                    if k not in input_step.tag_keys_available: del tags2[k]
                r[input_step.__class__.__name__] = input_step.stage.get_task_by(tags2)
            return r
        
        def make_INsD(input_steps,tags):
            #creates input_tasks_dict
            r = {
                 input_steps[0].__class__.__name__ : input_steps[0].stage.get_tasks_by(tags)
            }
            for input_step in input_steps[1:]:
                tags2 = tags.copy()
                for k in tags:
                    if k not in input_step.tag_keys_available: del tags2[k]
                r[input_step.__class__.__name__] = input_step.stage.get_tasks_by(tags2)
            return r
        
        multiple_input_steps = True if input_steps and len(input_steps) > 1 else False
        if multiple_input_steps:
            try:
                for input_step in input_steps: input_step.tag_keys_available = input_step.stage.get_all_tag_keys_used() #used by make_IND
            except IndexError as e:
                self.log.error("Input step has no tags.")
                raise e
                
        
        if not self.stage.successful:
            self.kwargs = kwargs
            self.new_tasks = []
            
            if _input_type == 'none':
                if _output_type == 'many':
                    G = self.none2many_cmd(*args,**kwargs)
                    self.add_many_tasks(G,{},[],{},)
            if _input_type == 'one':
                for input_task in input_steps[0].stage.tasks:
                    if _output_type == 'one':
                        if multiple_input_steps:
                            input_task_dict = make_IND(input_steps,input_task.tags)
                            r = self.multi_one2one_cmd(input_task_dict,*args,**kwargs)
                            self.add_one_task(r,input_task.tags,input_task_dict.values(),{'input_task_dict':input_task_dict})
                        else:
                            r = self.one2one_cmd(input_task,*args,**kwargs)
                            self.add_one_task(r,input_task.tags,[input_task],{'input_task':input_task})
                    elif _output_type == 'many':
                        if multiple_input_steps:
                            input_task_dict = make_IND(input_steps,input_task.tags)
                            G = self.multi_one2many_cmd(input_task_dict,*args,**kwargs)
                            self.add_many_tasks(G,input_task.tags,input_task_dict.values(),{'input_task_dict':input_task_dict})
                        else:
                            G = self.one2many_cmd(input_task,*args,**kwargs)
                            self.add_many_tasks(G,input_task.tags,[input_task],{'input_task':input_task})
                    
            elif _input_type == 'many':
                for tags,input_tasks in input_steps[0].stage.group_tasks_by(group_by):
                    if _output_type == 'one':
                        if multiple_input_steps:
                            input_tasks_dict = make_INsD(input_steps,tags)
                            r = self.multi_many2one_cmd(input_tasks_dict,*args,**kwargs)
                            self.add_one_task(r,tags,_unnest(input_tasks_dict.values()),{'input_task_dict':input_tasks})
                        else:
                            r = self.many2one_cmd(input_tasks,tags,*args,**kwargs)
                            self.add_one_task(r,tags,input_tasks,{'input_tasks':input_tasks})
                    elif _output_type == 'many':
                        if multiple_input_steps:
                            input_tasks_dict = make_INsD(input_steps,tags)
                            G = self.multi_one2many_cmd(input_tasks_dict,*args,**kwargs)
                            self.add_many_tasks(G,tags,_unnest(input_tasks_dict.values()),{'input_tasks_dict':input_tasks_dict})
                        else:
                            G = self.many2many_cmd(input_tasks,tags,*args,**kwargs)
                            self.add_many_tasks(G,tags,input_tasks,{'input_tasks':input_tasks})
            
            workflow.bulk_save_tasks(self.new_tasks)
            
        return self
        
    #############################
    # x2x
    #############################    
    
    def none2many(self,*args,**kwargs):
        """Basically a many2many, without the input_stage"""
        return self.__x2x(input_steps=None,_input_type='none',_output_type='many',*args,**kwargs)
    
    # many2many
    
    def many2many(self,input_step,group_by,*args,**kwargs):
        """
        Used when the parallelization is complex enough that the command should specify it.  The func:`self.many2many_cmd` will be passed
        the entire input_step rather than any tasks.
        
        :param group_by: Required.
        """
        return self.__x2x(input_steps=[input_step],group_by=group_by,_input_type='many',_output_type='many',*args,**kwargs)
    
    def multi_many2many(self,input_steps,group_by,*args,**kwargs):
        return self.__x2x(input_steps=input_steps,group_by=group_by,_input_type='many',_output_type='many',*args,**kwargs)
    # many2one
    
    def many2one(self,input_step,group_by,*args,**kwargs):
        """
        :param group_by: a list of tag keywords with which to parallelize input by.  see the keys parameter in :func:`Workflow.models.Workflow.group_tasks_by`.  An empty list will simply place all tasks in the stage into one group.
        """
        return self.__x2x(input_steps=[input_step],group_by=group_by,_input_type='many',_output_type='one',*args,**kwargs)
    
    def multi_many2one(self,input_steps,group_by,*args,**kwargs):
        return self.__x2x(input_steps=input_steps,group_by=group_by,_input_type='many',_output_type='one',*args,**kwargs)
    
    # one2one
    
    def one2one(self,input_step,*args,**kwargs):
        return self.__x2x(input_steps=[input_step],_input_type='one',_output_type='one',*args,**kwargs)
    
    def multi_one2one(self,input_steps,*args,**kwargs):
        return self.__x2x(input_steps=input_steps,_input_type='one',_output_type='one',*args,**kwargs)
    
    # one2many
    
    def one2many(self,input_step,*args,**kwargs):
        """
        Used when one input task becomes multiple output tasks
        """
        return self.__x2x(input_steps=[input_step],_input_type='one',_output_type='many',*args,**kwargs)
    
    def multi_one2many(self,input_steps,*args,**kwargs):
        return self.__x2x(input_steps=input_steps,_input_type='one',_output_type='many',*args,**kwargs)
    
    ########################################
    # x2x_cmds
    ########################################
    
    def none2many_cmd(self,*args,**kwargs):
        """
        :returns: {pcmd, pcmd_dict, add_tags}.  pcmd is required. pcmd_dict is a dictionary that pcmd will be .formated()ed with.  add_tags is required so that the task name stays unique.
        """
        raise NotImplementedError()
    
    def one2one_cmd(self,input_task,*args,**kwargs):
        """
        :returns: {pcmd, pcmd_dict}.  pcmd is required. pcmd_dict is a dictionary that pcmd will be .formated()ed with.
        """
        raise NotImplementedError()
    
    def multi_one2one_cmd(self,input_tasks_dict,*args,**kwargs):
        raise NotImplementedError()
    
    def many2one_cmd(self,input_tasks,tags,*args,**kwargs):
        """"
        The command to run
        
        :returns: {pcmd, pcmd_dict}.  pcmd is required. pcmd_dict is a dictionary that pcmd will be .formated()ed with.
        """
        raise NotImplementedError()
    
    def multi_many2one_cmd(self,input_tasks_dict,*args,**kwargs):
        raise NotImplementedError()
    
    
    def one2many_cmd(self,input_task,*args,**kwargs):
        """
        The command to run
        
        :yields: {pcmd, pcmd_dict, add_tags}.  pcmd is required.  pcmd_dict is a dictionary that pcmd will be .formated()ed with. add_tags is required so that the task name stays unique.
        """
        
        raise NotImplementedError()
    def multi_one2many_cmd(self,input_tasks_dict,*args,**kwargs):
        raise NotImplementedError()
    
    def many2many_cmd(self,input_step,*args,**kwargs):
        """
        The command to run
        
        :yields: {pcmd, pcmd_dict, add_tags, name}.  pcmd is required. pcmd_dict is a dictionary that pcmd will be .formated()ed with. add_tags is required so that the task name stays unique.
        """
        raise NotImplementedError()
    
    def multi_many2many_cmd(self,input_tasks_dict,*args,**kwargs):
        raise NotImplementedError()
    
    
    
    
    
    