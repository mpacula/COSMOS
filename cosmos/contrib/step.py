from cosmos.Cosmos.helpers import parse_cmd

workflow = None
settings = {}

def merge_dicts(x,y):
    for k,v in y.items(): x[k]=v
    return x

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
        d['settings'] = settings
        d['self'] = self
        return parse_cmd(string,**d)
    
    
    
    def add_node(self,pcmd,tags):
        """adds a node"""
        return self.batch.add_node(name = dict2node_name(tags),
                                    pcmd = pcmd,
                                    tags = tags,
                                    save = False,
                                    outputs = self.outputs,
                                    mem_req = self.mem_req,
                                    cpu_req = self.cpu_req)
        
    def __x2many(self,*args,**kwargs):
        """proxy for none2many and many2many"""
        many2many = True if 'input_batch' in kwargs else False
        if not self.batch.successful:
            new_nodes = []
            gnrtr = self.many2many_cmd(*args,**kwargs) if many2many else self.none2many_cmd(*args,**kwargs)
            for r in gnrtr:
                #Set defaults
                validate_dict_has_keys(r,['pcmd','new_tags'])
                pcmd_dict = r.setdefault('pcmd_dict',{})
                new_node = self.add_node(pcmd = self._parse_cmd2(r['pcmd'],pcmd_dict,tags=r['new_tags']),
                                         tags = r['new_tags'])
                new_nodes.append(new_node)
            workflow.bulk_save_nodes(new_nodes)
            workflow.run_wait(self.batch)
        return self.batch 
        
    def none2many(self,*args,**kwargs):
        """Basically a many2many, without the input_batch"""
        return self.__x2many(*args,**kwargs)
    
    def many2many(self,input_batch,*args,**kwargs):
        """
        Used when the parallelization is complex enough that the command should specify it.  The func:`self.many2many_cmd` will be passed
        the entire input_batch rather than any nodes.
        """
        return self.__x2many(input_batch,*args,**kwargs)
    
    def one2one(self,input_batch,*args,**kwargs):
        if not self.batch.successful:
            new_nodes = []
            for n in input_batch.nodes:
                r = self.one2one_cmd(input_node=n,*args,**kwargs)
                validate_dict_has_keys(r,['pcmd'])
                pcmd_dict = r.setdefault('pcmd_dict',{})
                new_node = self.add_node(pcmd = self._parse_cmd2(r['pcmd'],pcmd_dict,input_node=n,tags=n.tags),
                                         tags = n.tags)
                new_nodes.append(new_node)
            workflow.bulk_save_nodes(new_nodes)
            workflow.run_wait(self.batch)
        return self.batch
    
    def many2one(self,input_batch,group_by=[],*args,**kwargs):
        """
        
        :param group_by: a list of tag keywords with which to parallelize input by.  see the keys parameter in :func:`Workflow.models.Workflow.group_nodes_by`.  An empty list will simply place all nodes in the batch into one group.
        
        """
        #TODO make sure there are no name conflicts in kwargs and 'input_batch' and 'group_by'
        if not self.batch.successful:
            new_nodes = []
            for tags,input_nodes in input_batch.group_nodes_by(keys=group_by):
                r = self.many2one_cmd(input_nodes=input_nodes,tags=tags,*args,**kwargs)
                pcmd_dict = r.setdefault('pcmd_dict',{})
                new_node = self.add_node(pcmd = self._parse_cmd2(r['pcmd'],pcmd_dict,tags=tags),
                                         tags = tags)
                new_nodes.append(new_node)
            workflow.bulk_save_nodes(new_nodes)
            workflow.run_wait(self.batch)
        return self.batch
    
    def one2many(self,input_batch,*args,**kwargs):
        """
        Used when one input node becomes multiple output nodes
        """
        if not self.batch.successful:
            new_nodes = []
            for n in input_batch.nodes:
                for r in self.one2many_cmd(input_node=n,*args,**kwargs):
                    validate_dict_has_keys(r,['pcmd','add_tags'])
                    pcmd_dict = r.setdefault('pcmd_dict',{})
                    new_tags = merge_dicts(n.tags, r['add_tags'])
                    new_node = self.add_node(pcmd = self._parse_cmd2(r['pcmd'],pcmd_dict,input_node=n,tags=n.tags),
                                             tags = new_tags)
                    new_nodes.append(new_node)
            workflow.bulk_save_nodes(new_nodes)
            workflow.run_wait(self.batch)
        return self.batch
    
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
    
    
    
    
    
    