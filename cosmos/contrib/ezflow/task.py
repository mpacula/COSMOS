import flow,re
from decorators import pformat
import types, inspect
import hashlib

i = 0
def get_id():
    global i
    i +=1
    return i

files = []

class File(dict):
    
    def __init__(self,ext,path):
        self.id = get_id()
        self.path = path
        self.ext = ext
        
    @property
    def sha1sum(self):
        return hashlib.sha1(file(self.path).read())
    

class DefaultDict(dict):
    def __init__(self,root,*args,**kwargs):
        self.root = root
        return super(DefaultDict, self).__init__(*args,**kwargs) 
    
    def __getitem__(self, name):
        try:
            return super(DefaultDict, self).__getitem__(name)
        except KeyError:
            return "/{0}/{1}".format(self.root,name)
        
class Task(object):
    outputs = []
    inputs = []
    
    
    def __init__(self,tags={},outputs=[]):
        self.id = get_id()
        if not hasattr(self,'__verbose_name__'): self.__verbose_name__ = self.__class__.__name__
        self.tags = {}
        self.output_paths = DefaultDict('{0}/{1}'.format(self.__class__.__name__,self.id))
        for k,v in tags.items(): self.tags[k] = v 
#        for k,v in output_paths.items():
#            self.set_output_path(k,v)
            
#    def __getattribute__(self, name):
#        if name == 'cmd':
#            f = pformat(object.__getattribute__(self, name).im_func)
#            return types.MethodType(f,self,self.__class__)
#        else:
#            return object.__getattribute__(self, name)
    
    
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
        return "[{3}] {0}{1}\\n{2}".format(self.__verbose_name__,tags,self.pcmd,self.id)
        
    @property
    def pcmd(self):
        return self.map_cmd()
    
    def map_cmd(self):
        try:
            inputs = [ p.output_paths[self.inputs[0]] for p in self.parents ]
            empty_parameter_values = [None] * len(inspect.getargspec(self.cmd)[0][2:]) #this will become the parameter config
            return self.cmd(inputs,*empty_parameter_values)
        except TypeError as e:
            raise TypeError("{0}. - argspec of self.cmd is {1}".format(e,inspect.getargspec(self.cmd)))
        except IndexError as e:
            raise IndexError("{2}. - inputs for {0} are {1}".format(self,self.inputs,e))
        
    def cmd(self,*args,**kwargs):
        raise NotImplementedError()
    
    def set_output_path(self,name,path):
        if name not in self.outputs:
            raise Exception('Invalid output name')
        self.output_paths[name] = path
    
    def __str__(self):
        return '[{0}] {1} {2}'.format(self.id,self.__class__.__name__,self.tags)
    
