from Cosmos.helpers import parse_cmd

workflow = None
settings = {}

def merge_dicts(x,y):
    for k,v in y.items(): x[k]=v
    return x

def parse_cmd2(string,dictnry,**kwargs):
    'shortcut to combine with dict with kwargs and extra_parse_cmd_dict'
    d = merge_dicts(kwargs,dictnry)
    d['settings'] = settings
    return parse_cmd(string,**d)

def dict2node_name(d):
    s = ' '.join([ '{0}-{1}'.format(k,v) for k,v in d.items() ])
    if s == '': return 'node'
    return s
    
class Step():
    outputs = {}
    
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
        
    def _parse_command_string(self):
        pass
    
    def get_mem_req(self):
        return self.mem_req if hasattr(self,'mem_req') else None
    
    def one2one(self,input_batch,*args,**kwargs):
        if not self.batch.successful:
            for n in input_batch.nodes:
                r = self.one2one_cmd(input_node=n,*args,**kwargs)
                pcmd_dict = r.setdefault('pcmd_dict',{})
                self.batch.add_node(name = n.name,
                                    pcmd = parse_cmd2(r['pcmd'],pcmd_dict,input_node=n,tags=n.tags),
                                    tags = n.tags,
                                    outputs = self.outputs,
                                    mem_req = self.get_mem_req())
            workflow.run_wait(self.batch)
        return self.batch
    
    def many2one(self,input_batch,group_by=[],*args,**kwargs):
        """
        
        :param group_by: a list of tag keywords with which to parallelize input by.  see the keys parameter in :func:`Workflow.models.Workflow.group_nodes_by`.  An empty list will simply place all nodes in the batch into one group.
        
        """
        #TODO make sure there are no name conflicts in kwargs and 'input_batch' and 'group_by'
        if not self.batch.successful:
            for tags,input_nodes in input_batch.group_nodes_by(keys=group_by):
                r = self.many2one_cmd(input_nodes=input_nodes,tags=tags,*args,**kwargs)
                pcmd_dict = r.setdefault('pcmd_dict',{})
                self.batch.add_node(name = dict2node_name(tags),
                                    pcmd = parse_cmd2(r['pcmd'],pcmd_dict,tags=tags),
                                    tags = tags,
                                    outputs = self.outputs,
                                    mem_req = self.get_mem_req())
            workflow.run_wait(self.batch)
        return self.batch
    
    def one2many(self,input_batch,*args,**kwargs):
        """
        Used when one input node becomes multiple output nodes
        """
        if not self.batch.successful:
            for n in input_batch.nodes:
                for r in self.one2many_cmd(input_node=n,*args,**kwargs):
                    add_tags = r.setdefault('add_tags',{})
                    pcmd_dict = r.setdefault('pcmd_dict',{})
                    new_tags = merge_dicts(n.tags, add_tags)
                    self.batch.add_node(name = dict2node_name(new_tags),
                                        pcmd = parse_cmd2(r['pcmd'],pcmd_dict,input_node=n,tags=n.tags),
                                        tags = new_tags,
                                        outputs = self.outputs,
                                        mem_req = self.get_mem_req())
            workflow.run_wait(self.batch)
        return self.batch
    
    def many2many(self,input_batch,*args,**kwargs):
        """
        Used when the parallelization is complex enough that the command should specify it.  The func:`self.many2many_cmd` will be passed
        the entire input_batch rather than any nodes.
        """
        if not self.batch.successful:
            for r in self.many2many_cmd(input_batch=input_batch,*args,**kwargs):
                #Set defaults
                pcmd_dict = r.setdefault('pcmd_dict',{})
                new_tags = r.setdefault('new_tags',{})
                name = r.setdefault('name',dict2node_name(r['new_tags']))
                self.batch.add_node(name = name,
                                    pcmd = parse_cmd2(r['pcmd'],pcmd_dict,input_batch=input_batch,tags=new_tags),
                                    tags = new_tags,
                                    outputs = self.outputs,
                                    mem_req = self.get_mem_req())
            workflow.run_wait(self.batch)
        return self.batch
    
    
    def one2one_cmd(self,input_node,*args,**kwargs):
        """
        The command to run
        
        :returns: {pcmd, pcmd_format_dictionary}.  pcmd is required.
        """
        raise NotImplementedError()
    
    def many2one_cmd(self,input_nodes,tags,*args,**kwargs):
        """"
        The command to run
        
        :returns: {pcmd, pcmd_format_dictionary}.  pcmd is required.
        """
        raise NotImplementedError()
    
    def one2many_cmd(self,input_node,*args,**kwargs):
        """
        The command to run
        
        :yields: {pcmd, pcmd_format_dictionary, add_tags}.  pcmd is required.
        """
        raise NotImplementedError()
    
    def many2many_cmd(self,input_batch,*args,**kwargs):
        """
        The command to run
        
        :yields: {pcmd, pcmd_format_dictionary, new_tags, name}.  pcmd is required.
        """
        raise NotImplementedError()
    
    
    
    
    
    