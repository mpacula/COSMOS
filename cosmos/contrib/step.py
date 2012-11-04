from cosmos.Cosmos.helpers import parse_cmd
from django.core.exceptions import ValidationError

workflow = None

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


def _unnest(self,a_list):
    """
    unnests a list
    
    .. example::
    >>> _unnest([1,2,[3,4],[5]])
    [1,2,3,4,5]
    """
    return [ item for items in a_list for item in items ]

def dict2node_name(d):
    s = ' '.join([ '{0}-{1}'.format(k,v) for k,v in d.items() ])
    if s == '': return 'node'
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
    new_nodes = []
    kwargs = {}
    
    def __init__(self,name=None,hard_reset=False,**kwargs):
        """
        :param hard_reset: Deletes the old batch before creating this one  
        """
        if workflow is None:
            raise Exception('Set the parameter step.workflow to your Workflow before adding steps.')
        
        if name is None:
            name = type(self)
        self.name = name 
        
        self.batch = workflow.add_batch(self.name,hard_reset=hard_reset)


    def _parse_cmd2(self,string,dictnry,**kwargs):
        'shortcut to combine with dict with kwargs and extra_parse_cmd_dict'
        #TODO throw an error if there are key conflicts
        d = merge_dicts(kwargs,dictnry)
        if self not in d: d['self'] = self
        return parse_cmd(string,**d)
    
    
    
    def __add_node_to_batch(self,pcmd,pcmd_dict,tags,parents):
        """adds a node"""
        return self.batch.add_node(name = '',
                                   pcmd = self._parse_cmd2(pcmd,pcmd_dict,tags=tags),
                                    tags = tags,
                                    save = False,
                                    parents = parents,
                                    outputs = self.outputs,
                                    mem_req = self.mem_req,
                                    cpu_req = self.cpu_req)
      
    def __x2x(self,input_steps, input_type, output_type,group_by=None,*args,**kwargs):
        """
        proxy for all algorithms
        
        :param input_type: 'many' or 'one' or 'none'
        :param ouput_type: 'many' or 'one'
        """
        multiple_input_steps = True if input_steps and len(input_steps) > 1 else False
        
        if not self.batch.successful:
            self.kwargs = kwargs
            self.new_nodes = []
            
            if input_type == 'none':
                if output_type == 'many':
                    G = self.none2many_cmd(*args,**kwargs)
                    self.add_many_nodes(G,{},[],{})
            if input_type == 'one':
                for input_node in input_steps[0].batch.nodes:
                    if output_type == 'one':
                        if multiple_input_steps:
                            input_nodes_dict = dict([ (s.__class__.__name__,s.batch.get_node_by(input_node.tags)) for s in input_steps ])
                            r = self.multi_one2one_cmd(input_nodes_dict,*args,**kwargs)
                            self.add_one_node(r,input_node.tags,input_nodes_dict.values(),{'input_nodes_dict':input_nodes_dict})
                        else:
                            r = self.one2one_cmd(input_node,*args,**kwargs)
                            self.add_one_node(r,input_node.tags,[input_node],{'input_node':input_node})
                    elif output_type == 'many':
                        G = self.one2many_cmd(input_node,*args,**kwargs)
                        self.add_many_nodes(G,input_node.tags,[input_node],{'input_node':input_node})
                    
            elif input_type == 'many':
                for tags,input_nodes in input_steps[0].batch.group_nodes_by(group_by):
                    if output_type == 'one':
                        r = self.many2one_cmd(input_nodes,tags,*args,**kwargs)
                        self.add_one_node(r,tags,input_nodes,{'input_nodes':input_nodes})
                    elif output_type == 'many':
                        G = self.many2many_cmd(input_nodes,tags,*args,**kwargs)
                        self.add_many_nodes(G,tags,input_nodes,{'input_nodes':input_nodes})
            
            workflow.bulk_save_nodes(self.new_nodes)
            #workflow.run_wait(self.batch)
        return self
    
    def add_many_nodes(self,G,tags,parents,extra_pcmd_dict):
        #if type(parents) != list: raise StepError('parents parameter must be a list.  parents is {0} and set to {1}'.format(type(parents),parents))
        for r in G:
            validate_dict_has_keys(r,['pcmd','add_tags'])
            new_node = self.__add_node_to_batch(pcmd = r['pcmd'],
                                     pcmd_dict = merge_mdicts(self.kwargs,{'tags':tags},extra_pcmd_dict,r.setdefault('pcmd_dict',{})),
                                     tags = merge_dicts(tags,r['add_tags']),
                                     parents = parents)
            self.new_nodes.append(new_node)
    
    def add_one_node(self,r,tags,parents,extra_pcmd_dict):
        #if type(parents) != list: raise StepError('parents parameter must be a list.  parents type is {0} and set to {1}'.format(type(parents),parents))
        validate_dict_has_keys(r,['pcmd'])
        add_tags = r.setdefault('add_tags',{})
        new_node = self.__add_node_to_batch(pcmd = r['pcmd'],
                             pcmd_dict = merge_mdicts(self.kwargs,{'tags':tags},extra_pcmd_dict,r.setdefault('pcmd_dict',{}),add_tags),
                             tags = merge_dicts(add_tags,tags),
                             parents = parents)
        self.new_nodes.append(new_node)
        
        
    def none2many(self,*args,**kwargs):
        """Basically a many2many, without the input_batch"""
        return self.__x2x(input_steps=None,input_type='none',output_type='many',*args,**kwargs)
    
    def many2many(self,input_step=None,input_steps=None,group_by=None,*args,**kwargs):
        """
        Used when the parallelization is complex enough that the command should specify it.  The func:`self.many2many_cmd` will be passed
        the entire input_step rather than any nodes.
        
        :param group_by: Required.
        """
        if input_step == input_steps:
            raise ValidationError('The parameter input_step or input_steps is required.  Both cannot be used.')
        
        input_steps = input_steps if input_steps else [input_step]
        return self.__x2x(input_steps=input_steps,group_by=group_by,input_type='many',output_type='many',*args,**kwargs)
    
    def one2one(self,input_step=None,input_steps=None,*args,**kwargs):
        """
        :param input_step: The input batch.  Required if input_steps is not set.  Do not set both input_step and input_steps.
        :param input_steps: A list of input batches.  Will iterate using the first batch the list, and pass a list of input_nodes all with the same tags to the one2one_cmd.  Optional.
        """
        if input_step == input_steps:
            raise ValidationError('The parameter input_step or input_steps is required.  Both cannot be used.')
        
        input_steps = input_steps if input_steps else [input_step]
        return self.__x2x(input_steps=input_steps,input_type='one',output_type='one',*args,**kwargs)
    
    def many2one(self,input_step=None,input_steps=None,group_by=None,*args,**kwargs):
        """
        
        :param group_by: a list of tag keywords with which to parallelize input by.  see the keys parameter in :func:`Workflow.models.Workflow.group_nodes_by`.  An empty list will simply place all nodes in the batch into one group.
        
        """
        if input_step == input_steps:
            raise ValidationError('The parameter input_step or input_steps is required.  Both cannot be used.')
        
        input_steps = input_steps if input_steps else [input_step]
        return self.__x2x(input_steps=input_steps,group_by=group_by,input_type='many',output_type='one',*args,**kwargs)
    
    
    def one2many(self,input_step=None,input_steps=None,*args,**kwargs):
        """
        Used when one input node becomes multiple output nodes
        """
        if input_step == input_steps:
            raise ValidationError('The parameter input_step or input_steps is required.  Both cannot be used.')
        
        input_steps = input_steps if input_steps else [input_step]
        return self.__x2x(input_steps=input_steps,input_type='one',output_type='many',*args,**kwargs)
    
    def none2many_cmd(self,*args,**kwargs):
        """
        The command to run
        
        :returns: {pcmd, pcmd_dict, add_tags}.  pcmd is required. pcmd_dict is a dictionary that pcmd will be .formated()ed with.  add_tags is required so that the node name stays unique.
        """
        raise NotImplementedError()
    
    def one2one_cmd(self,input_node,*args,**kwargs):
        """
        The command to run
        
        :returns: {pcmd, pcmd_dict}.  pcmd is required. pcmd_dict is a dictionary that pcmd will be .formated()ed with.
        """
        raise NotImplementedError()
    
    def multi_one2one_cmd(self,input_nodes_dict,*args,**kwargs):
        """
        :returns: {pcmd, pcmd_dict}
        """
        raise NotImplementedError()
    
    def many2one_cmd(self,input_nodes,tags,*args,**kwargs):
        """"
        The command to run
        
        :returns: {pcmd, pcmd_dict}.  pcmd is required. pcmd_dict is a dictionary that pcmd will be .formated()ed with.
        """
        raise NotImplementedError()
    
    def one2many_cmd(self,input_node,*args,**kwargs):
        """
        The command to run
        
        :yields: {pcmd, pcmd_dict, add_tags}.  pcmd is required.  pcmd_dict is a dictionary that pcmd will be .formated()ed with. add_tags is required so that the node name stays unique.
        """
        raise NotImplementedError()
    
    def many2many_cmd(self,input_step,*args,**kwargs):
        """
        The command to run
        
        :yields: {pcmd, pcmd_dict, add_tags, name}.  pcmd is required. pcmd_dict is a dictionary that pcmd will be .formated()ed with. add_tags is required so that the node name stays unique.
        """
        raise NotImplementedError()
    
    
    
    
    
    