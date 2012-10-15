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

def dict2str(d):
    return ' '.join([ '{0}-{1}'.format(k,v) for k,v in d.items() ])
    
class Step():
    outputs = {}
    
    def __init__(self,name=None,hard_reset=False,**kwargs):
        """
        :param \*\*kwargs:  
        """
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
                cmd = self.one2one_cmd(input_node=n,*args,**kwargs)
                self.batch.add_node(name = n.name,
                                    pcmd = parse_cmd2(*cmd,input_node=n,tags=n.tags),
                                    tags = n.tags,
                                    outputs = self.outputs,
                                    mem_req = self.get_mem_req())
            workflow.run_wait(self.batch)
        return self.batch
    
    def many2one(self,input_batch,group_by,*args,**kwargs):
        """ :param group_by: a list of tag keywords with which to parallelize input by"""
        #TODO make sure there are no name conflicts in kwargs and 'input_batch' and 'group_by'
        if not self.batch.successful:
            for tags,input_nodes in input_batch.group_nodes_by(*group_by):
                cmd, cmd_dict = self.many2one_cmd(input_nodes=input_nodes,tags=tags,*args,**kwargs)
                self.batch.add_node(name = dict2str(tags),
                                    pcmd = parse_cmd2(cmd,cmd_dict,tags=tags),
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
                for cmd,cmd_dict,add_tags in self.one2many_cmd(input_node=n,*args,**kwargs):
                    new_tags = merge_dicts(n.tags, add_tags)
                    self.batch.add_node(name = dict2str(new_tags),
                                        pcmd = parse_cmd2(cmd,cmd_dict,input_node=n,tags=n.tags),
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
            for cmd,cmd_dict,new_tags in self.many2many_cmd(input_batch=input_batch,*args,**kwargs):
                self.batch.add_node(name = dict2str(new_tags),
                                    pcmd = parse_cmd2(cmd,cmd_dict,input_batch=input_batch,tags=new_tags),
                                    tags = new_tags,
                                    outputs = self.outputs,
                                    mem_req = self.get_mem_req())
            workflow.run_wait(self.batch)
        return self.batch
    
    
    def one2one_cmd(self,input_node,*args,**kwargs):
        """
        The command to run
        
        :returns: (pcmd, pcmd_format_dictionary)
        """
        raise NotImplementedError()
    
    def many2one_cmd(self,input_nodes,tags,*args,**kwargs):
        """"
        The command to run
        
        :returns: (pcmd, pcmd_format_dictionary)
        """
        raise NotImplementedError()
    
    def one2many_cmd(self,input_node,*args,**kwargs):
        """
        The command to run
        
        :yields: [(pcmd, pcmd_format_dictionary, add_tags)]
        """
        raise NotImplementedError()
    
    def many2many_cmd(self,input_batch,*args,**kwargs):
        """
        The command to run
        
        :yields: [(pcmd, pcmd_format_dictionary, new_tags).  new_tags cannot be empty.]
        """
        raise NotImplementedError()
    
    
    
    
    
    