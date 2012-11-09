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
    
    
    
    def add_node(self,pcmd,pcmd_dict,tags,parents):
        """adds a node"""
        return self.batch.add_node(name = '',
                                   pcmd = self._parse_cmd2(pcmd,pcmd_dict,tags=tags),
                                    tags = tags,
                                    save = False,
                                    parents = parents,
                                    outputs = self.outputs,
                                    mem_req = self.mem_req,
                                    cpu_req = self.cpu_req)
        
    def __x2x(self,input_batches, input_type, output_type,group_by=None,*args,**kwargs):
        """
        proxy for all algorithms
        
        :param input_type: 'many' or 'one' or 'none'
        :param ouput_type: 'many' or 'one'
        """
        multi_batch_input = True if input_batches and len(input_batches) > 1 else False
         
        if not self.batch.successful:
            new_nodes = []
            
            #submit to cmd
            if output_type == 'many':
                if input_type == 'none':
                    gnrtr = self.none2many_cmd(*args,**kwargs)
                    for r in gnrtr:
                        validate_dict_has_keys(r,['pcmd','new_tags'])
                        new_node = self.add_node(pcmd = r['pcmd'],
                                                 pcmd_dict = merge_dicts(kwargs,r.setdefault('pcmd_dict',{})),
                                                 tags = r['new_tags'],
                                                 parents = [])
                        new_nodes.append(new_node)
                        
                elif input_type == 'one':
                    gnrtr = self.one2many_cmd(input_batches[0],*args,**kwargs)
                    for r in gnrtr:
                        validate_dict_has_keys(r,['pcmd','add_tags'])
                        new_node = self.add_node(pcmd = r['pcmd'],
                                                 pcmd_dict = merge_dicts(kwargs,r.setdefault('pcmd_dict',{})),
                                                 tags = r['add_tags'],
                                                 parents = input_batches[0])
                        new_nodes.append(new_node)
                        
                elif input_type == 'many':
                    for input_nodes,tags in input_batches[0].group_nodes_by(group_by):
                        gnrtr = self.many2one_cmd(input_nodes,tags,*args,**kwargs)
                        for r in gnrtr:
                            validate_dict_has_keys(r,['pcmd'])
                            new_node = self.add_node(pcmd = r['pcmd'],
                                                     pcmd_dict = merge_mdicts(kwargs,{'tags':tags},r.setdefault('pcmd_dict',{})),
                                                     tags = merge_dicts(tags,r['new_tags']),
                                                     parents = [input_nodes])
                            new_nodes.append(new_node)
                
            elif output_type == 'one':
                if input_type == 'one':
                    for n in input_batches[0].nodes:
                        if multi_batch_input:
                            input_nodes = [n] + [ b.get_node_by(n.tags) for b in input_batches[1:] ]
                            r = self.multi_one2one_cmd(input_nodes=input_nodes,*args,**kwargs)
                            validate_dict_has_keys(r,['pcmd'])
                            extra_pcmd_dict = {'input_nodes':input_nodes}
                            parents = input_nodes
                        else:
                            r = self.one2one_cmd(input_node=n,*args,**kwargs)
                            extra_pcmd_dict = {'input_node':n}
                            parents = [n]
                        validate_dict_has_keys(r,['pcmd'])
                        new_node = self.add_node(pcmd = r['pcmd'],
                                                 pcmd_dict = merge_mdicts(kwargs,extra_pcmd_dict,r.setdefault('pcmd_dict',{})),
                                                 tags = n.tags,
                                                 parents = parents)
                        new_nodes.append(new_node)
                elif input_type == 'many':
                    for input_nodes,tags in input_batches[0].group_nodes_by(group_by):
                        r = self.many2one_cmd(input_nodes,tags,*args,**kwargs)
                        validate_dict_has_keys(r,['pcmd','new_tags'])
                        new_node = self.add_node(pcmd = r['pcmd'],
                                                 pcmd_dict = merge_mdicts(kwargs,{'tags':tags},r.setdefault('pcmd_dict',{})),
                                                 tags = merge_dicts(tags,r['new_tags']),
                                                 parents = [input_nodes])
                        new_nodes.append(new_node)
            
            workflow.bulk_save_nodes(new_nodes)
            #workflow.run_wait(self.batch)
        return self.batch 
        
    def none2many(self,*args,**kwargs):
        """Basically a many2many, without the input_batch"""
        return self.__x2x(input_batches=None,input_type='none',output_type='many',*args,**kwargs)
    
    def many2many(self,input_batch=None,input_batches=None,group_by=None,*args,**kwargs):
        """
        Used when the parallelization is complex enough that the command should specify it.  The func:`self.many2many_cmd` will be passed
        the entire input_batch rather than any nodes.
        
        :param group_by: Required.
        """
        if input_batch == input_batches:
            raise ValidationError('The parameter input_batch or input_batches is required.  Both cannot be used.')
        
        input_batches = input_batches if input_batches else [input_batch]
        return self.__x2x(input_batches=input_batches,group_by=group_by,input_type='many',output_type='many',*args,**kwargs)
    
    def one2one(self,input_batch=None,input_batches=None,*args,**kwargs):
        """
        :param input_batch: The input batch.  Required if input_batches is not set.  Do not set both input_batch and input_batches.
        :param input_batches: A list of input batches.  Will iterate using the first batch the list, and pass a list of input_nodes all with the same tags to the one2one_cmd.  Optional.
        """
        if input_batch == input_batches:
            raise ValidationError('The parameter input_batch or input_batches is required.  Both cannot be used.')
        
        input_batches = input_batches if input_batches else [input_batch]
        return self.__x2x(input_batches=input_batches,input_type='one',output_type='one',*args,**kwargs)
    
    def many2one(self,input_batch=None,input_batches=None,group_by=None,*args,**kwargs):
        """
        
        :param group_by: a list of tag keywords with which to parallelize input by.  see the keys parameter in :func:`Workflow.models.Workflow.group_nodes_by`.  An empty list will simply place all nodes in the batch into one group.
        
        """
        if input_batch == input_batches:
            raise ValidationError('The parameter input_batch or input_batches is required.  Both cannot be used.')
        
        input_batches = input_batches if input_batches else [input_batch]
        return self.__x2x(input_batches=input_batches,group_by=group_by,input_type='many',output_type='one',*args,**kwargs)
    
    
    def one2many(self,input_batch=None,input_batches=None,*args,**kwargs):
        """
        Used when one input node becomes multiple output nodes
        """
        if input_batch == input_batches:
            raise ValidationError('The parameter input_batch or input_batches is required.  Both cannot be used.')
        
        input_batches = input_batches if input_batches else [input_batch]
        return self.__x2x(input_batches=input_batches,input_type='one',output_type='many',*args,**kwargs)
    
    def none2many_cmd(self,*args,**kwargs):
        """
        The command to run
        
        :returns: {pcmd, pcmd_dict, new_tags}.  pcmd is required. pcmd_dict is a dictionary that pcmd will be .formated()ed with.  new_tags is required so that the node name stays unique.
        """
        raise NotImplementedError()
    
    def one2one_cmd(self,input_node,*args,**kwargs):
        """
        The command to run
        
        :returns: {pcmd, pcmd_dict}.  pcmd is required. pcmd_dict is a dictionary that pcmd will be .formated()ed with.
        """
        raise NotImplementedError()
    
    def multi_one2one_cmd(self,input_nodes,*args,**kwargs):
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
    
    def many2many_cmd(self,input_batch,*args,**kwargs):
        """
        The command to run
        
        :yields: {pcmd, pcmd_dict, new_tags, name}.  pcmd is required. pcmd_dict is a dictionary that pcmd will be .formated()ed with. new_tags is required so that the node name stays unique.
        """
        raise NotImplementedError()
    
    
    
    
    
    