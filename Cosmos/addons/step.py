from Cosmos.helpers import parse_cmd

workflow = None

def merge_dicts(x,y):
    for k,v in y.items(): x[k]=v
    return x

def parse_cmd2(string,dictnry,**kwargs):
    'shortcut to combine with dict with kwargs'
    return parse_cmd(string,**merge_dicts(kwargs,dictnry))

def dict2str(d):
    return ' '.join([ '{0}-{1}'.format(k,v) for k,v in d.items() ])
    
class Step():
    outputs = {}
    
    def __init__(self,name=None,**kwargs):
        """
        :param \*\*kwargs:  
        """
        if name is None:
            name = type(self)
        self.name = name 
        
        self.batch = workflow.add_batch(self.name)
        
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
    
    def many2one(self,input_batch,group_by,*args,**kwargs):
        """ :param group_by: a list of tag keywords with which to parallelize input by"""
        for tags,input_nodes in input_batch.group_nodes_by(*group_by):
            cmd, cmd_dict = self.many2one_cmd(input_nodes=input_nodes)
            self.batch.add_node(name = dict2str(tags),
                                pcmd = parse_cmd2(cmd,cmd_dict,tags=tags),
                                tags = tags,
                                outputs = self.outputs,
                                mem_req = self.get_mem_req())
        workflow.run_wait(self.batch)
        return self.batch
    
    
    def one2one_cmd(self,input_node,*args,**kwargs):
        "The command to run"
        raise NotImplementedError()
    
    def many2one_cmd(self,input_node,*args,**kwargs):
        "The command to run"
        raise NotImplementedError()
    
    def one2many_cmd(self,input_node,*args,**kwargs):
        "The command to run"
        raise NotImplementedError()
    
    
    
    
    
    