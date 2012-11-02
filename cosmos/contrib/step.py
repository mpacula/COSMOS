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

def merge_mdicts(l):
    """
    Merges a list of dictionaries.  Right most keys take precedence
    """
    return reduce(merge_dicts,l)

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
    
    
    
    def add_node(self,pcmd,tags,parents):
        """adds a node"""
        return self.batch.add_node(name = dict2node_name(tags),
                                    pcmd = pcmd,
                                    tags = tags,
                                    parents = parents,
                                    save = False,
                                    outputs = self.outputs,
                                    mem_req = self.mem_req,
                                    cpu_req = self.cpu_req)
        
    def __x2many(self,*args,**kwargs):
        """proxy for none2many and many2many"""
        
    def none2many(self,*args,**kwargs):
        """Basically a many2many, without the input_batch"""
        if not self.batch.successful:
            new_nodes = []
            for r in self.none2many_cmd(*args,**kwargs):
                #Set defaults
                validate_dict_has_keys(r,['pcmd','new_tags'])
                pcmd_dict = r.setdefault('pcmd_dict',{})
                new_node = self.add_node(pcmd = self._parse_cmd2(r['pcmd'],merge_dicts(kwargs,pcmd_dict),tags=r['new_tags']),
                                         tags = r['new_tags'],
                                         parents = [])
                new_nodes.append(new_node)
            workflow.bulk_save_nodes(new_nodes)
            workflow.run_wait(self.batch)
        return self.batch 
    
    def many2many(self,input_batch=None,input_batches=None,group_by,*args,**kwargs):
        """
        Used when the parallelization is complex enough that the command should specify it.  The func:`self.many2many_cmd` will be passed
        the entire input_batch rather than any nodes.
        """
        if not self.batch.successful:
            new_nodes = []
            for tags,input_nodes in input_batch.group_nodes_by(keys=group_by):
                for r in self.many2many_cmd(input_nodes=input_nodes,tags=group_by,*args,**kwargs):
                    #Set defaults
                    validate_dict_has_keys(r,['pcmd','new_tags'])
                    pcmd_dict = r.setdefault('pcmd_dict',{})
                    tags = merge_dicts(r['new_tags'],group_by)
                    new_node = self.add_node(pcmd = self._parse_cmd2(r['pcmd'],merge_dicts(kwargs,pcmd_dict),tags=tags),
                                             tags = tags,
                                             parents = input_nodes)
                    new_nodes.append(new_node)
                workflow.bulk_save_nodes(new_nodes)
                workflow.run_wait(self.batch)
            return self.batch 
    
    def one2one(self,input_batch=None,input_batches=None,*args,**kwargs):
        """
        :param input_batch: The input batch.  Required if input_batches is not set.  Do not set both input_batch and input_batches.
        :param input_batches: A list of input batches.  Will iterate using the first batch the list, and pass a list of input_nodes all with the same tags to the one2one_cmd.  Optional.
        """
        #Validation
        if input_batch == input_batches:
            raise ValidationError('The parameter input_batch or input_batches is required.  Both cannot be used.')
        if not self.batch.successful:
            new_nodes = []
            multi_input = input_batches is not None
            primary_input_batch = input_batches[0] if multi_input else input_batch
            for n in primary_input_batch.nodes:
                if multi_input:
                    input_nodes = [ b.get_node_by(n.tags) for b in input_batches ]
                    r = self.one2one_cmd(input_nodes=input_nodes,*args,**kwargs)
                    pcmd_dict = {'pcmd_dict':r.setdefault('pcmd_dict',{})}
                    pcmd_dict = merge_dicts({'input_nodes':input_nodes},pcmd_dict)
                    parents = input_nodes
                else:
                    r = self.one2one_cmd(input_node=n,*args,**kwargs)
                    pcmd_dict = {'pcmd_dict':r.setdefault('pcmd_dict',{})}
                    pcmd_dict = merge_dicts({'input_node':n},pcmd_dict)
                    parents = [n]
                    
                validate_dict_has_keys(r,['pcmd'])
                new_node = self.add_node(pcmd = self._parse_cmd2(r['pcmd'],merge_dicts(kwargs,pcmd_dict),tags=n.tags),
                                         tags = n.tags,
                                         parents = parents)
                new_nodes.append(new_node)
            workflow.bulk_save_nodes(new_nodes)
            workflow.run_wait(self.batch)
        return self.batch
    
    def many2one(self,input_batch,group_by,*args,**kwargs):
        """
        
        :param group_by: a list of tag keywords with which to parallelize input by.  see the keys parameter in :func:`Workflow.models.Workflow.group_nodes_by`.  An empty list will simply place all nodes in the batch into one group.
        
        """
        #TODO make sure there are no name conflicts in kwargs and 'input_batch' and 'group_by'
        if not self.batch.successful:
            new_nodes = []
            for tags,input_nodes in input_batch.group_nodes_by(keys=group_by):
                r = self.many2one_cmd(input_nodes=input_nodes,tags=tags,*args,**kwargs)
                pcmd_dict = r.setdefault('pcmd_dict',{})
                new_node = self.add_node(pcmd = self._parse_cmd2(r['pcmd'],merge_dicts(kwargs,pcmd_dict),tags=tags),
                                         tags = tags,
                                         parents = input_nodes)
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
                    new_node = self.add_node(pcmd = self._parse_cmd2(r['pcmd'],merge_dicts(kwargs,pcmd_dict),input_node=n,tags=n.tags),
                                             tags = new_tags,
                                             parents = [n])
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
    
    
    
    
    
    