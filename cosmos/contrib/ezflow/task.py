import flow,re

i = 0
def get_id():
    global i
    i +=1
    return i

class DefaultDict( dict ):
    def __init__(self,*args,**kwargs):
        return super(DefaultDict, self).__init__(*args,**kwargs) 
    
    def __getitem__(self, name):
        try:
            return super(DefaultDict, self).__getitem__(name)
        except KeyError:
            return "['{0}' NA]".format(name)
class Task:
    outputs = []
    inputs = []
    
    def __init__(self,tags=DefaultDict(),outputs={}):
        self.id = get_id()
        self.tags = DefaultDict()
        self.output_paths = DefaultDict()
        for k,v in tags.items(): self.tags[k] = v 
        for k,v in outputs.items():
            self.set_output_path(k,v)
    
    @property
    def parents(self):
        return flow.DAG.predecessors(self)
    
    @property
    def parent(self):
        ps = flow.DAG.predecessors(self)
        if len(ps) > 1:
            raise Exception('{0} has more than one parent.  The parents are: {1}'.format(self,self.parents))
        elif len(ps) == 0:
            raise Exception('{0} has no parents'.format(self))
        else:
            return ps[0]
    @property
    def label(self):
        tags = '' if len(self.tags) == 0 else "\\n {0}".format("\\n".join(["{0}: {1}".format(re.sub('interval','chr',k),v) for k,v in self.tags.items() ]))
        return "{0}{1}\\n{2}".format(self.__verbose_name__ if hasattr(self,'__verbose_name__') else self.__class__.__name__,tags,self.pcmd)
        
    @property
    def pcmd(self):
        return self.map_cmd()
    
    def map_cmd(self):
        try:
            if len(self.parents) > 1:
                inputs = [ p.output_paths[self.inputs[0]] for p in self.parents ]
                return self.cmd(inputs)
            return self.cmd(self.parent.output_paths[self.inputs[0]])
        except IndexError:
            print "inputs for {0} are {1}".format(self,self.inputs)
            raise
        except TypeError:
            print "{0}.map_cmd failed with TypeError".format(self)
            raise
        
    def cmd(self,*args,**kwargs):
        raise NotImplementedError()
    
    def set_output_path(self,name,path):
        if name not in self.outputs:
            raise Exception('Invalid output name')
        self.output_paths[name] = path
    
    def __str__(self):
        return '[{0}] {1} {2}'.format(self.id,self.__class__.__name__,self.tags)
    
