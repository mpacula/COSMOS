import dag,re
from decorators import pformat
import types, inspect
import hashlib

i = 0
def get_id():
    global i
    i +=1
    return i

files = []

class ExpectedError(Exception): pass
class ToolError(Exception): pass
class GetOutputError(Exception): pass

class TaskFile(object):
    """
    Task File
    """

    def __init__(self,path=None,fmt=None,name=None,*args,**kwargs):
        self.id = get_id()
        self.path = path
        self.fmt = fmt if fmt else re.search('\.(.+?)$',path).group(1)
        if name is None:
            self.name = self.fmt
        return super(TaskFile,self).__init__(*args,**kwargs)
        
    @property
    def sha1sum(self):
        return hashlib.sha1(file(self.path).read())
    
    def __str__(self):
        s = self.path if self.path else '(na.{0})'.format(self.fmt)
        return "#F[{0}]:{1}".format(self.id,s)

#class DefaultDict(dict):
#    def __init__(self,root,*args,**kwargs):
#        self.root = root
#        return super(DefaultDict, self).__init__(*args,**kwargs) 
#    
#    def __getitem__(self, name):
#        try:
#            return super(DefaultDict, self).__getitem__(name)
#        except KeyError:
#            return "/{0}/{1}".format(self.root,name)
#class AttributeDict(dict):
#    """
#    Provides access to attributes using dict.attr
#    """
#    def __getattr__(self,name):
#        if not hasattr(self,name):
#            return self.__getitem__(name)
#        return super(dict,self).__getattr__(name)
        
class Tool(object):
    """
    A Tool
    
    If initialized with a single argument, it becomes a factory that produces a class who's stage_name is that argument.
    Otherwise, intialize with keyword arguments to make it behave like a regular class.
    
    :property stage_name: (str) The name of this Tool's stage.  Defaults to Tool.__verbose__
    :property DAG: The DAG that is keeping track of this Tool
    :property id: A unique identifier.  Useful for debugging.
    :property output_taskFiles: This Tool's TaskFiles
    :property tags: This Tool's tags
    :property inputs: a list of input names, must be specified by user
    :property outputs: a list of output names, must be specified by user
    """
    NOOP = False #set to True if this Task should never actually be submitted to the Cosmos Workflow
    #stage_name
    
    def __init__(self,DAG,tags={},output_taskFiles=[]):
        """
        :param output_taskFiles: A list, or a dict of TaskFiles.  if a list, it will be converted to a dict who's keywords are the TaskFiles' fmt. 
        
        .. note:: because of the special __new__, Task must be constructed using keyword in the parameters only.
        """
        self.output_taskFiles = {}
        if not hasattr(self,'__verbose__'): self.__verbose__ = self.stage_name
        self.DAG = DAG
        
        self.id = get_id()
        for tf in output_taskFiles:
            self.output_taskFiles[tf.fmt] = tf
            
        for output_ext in self.outputs:
            if output_ext not in self.output_taskFiles:
                self.output_taskFiles[output_ext] = TaskFile(None,output_ext)
        self.tags = tags
        
    
    def get_output(self,name):
        try:
            return self.output_taskFiles[name]
        except KeyError:
            raise GetOutputError("{0} does not exist in {1}.output_taskFiles".format(name,self))
#        s = ', '.join(map(lambda x: str(x),self.output_taskFiles))
#        r = filter(lambda x: x.fmt == ext,self.output_taskFiles)
#        if len(r) == 1:
#            return r[0]
#        elif len(r) == 0:
#            return '/Output/{0}/not/set.'.format(ext,s)
#            raise ExpectedError('No output with ext {0}.  outputs are: {1}'.format(ext,s))
#        elif len(r) > 1:
#            raise ExpectedError('More than one output with ext {0}'.format(0))
         
    def __new__(cls,stage_name=None,*args,**kwargs):
        if stage_name:
            cls.stage_name = stage_name
            return cls
        else:
            cls.stage_name = cls.__name__
            return super(Tool, cls).__new__(cls, *args, **kwargs)
            
#    def __getattribute__(self, name):
#        if name == 'cmd':
#            f = pformat(object.__getattribute__(self, name).im_func)
#            return types.MethodType(f,self,self.__class__)
#        else:
#            return object.__getattribute__(self, name)
    
    
    @property
    def parents(self):
        return self.DAG.G.predecessors(self)
    
    @property
    def parent(self):
        ps = self.DAG.G.predecessors(self)
        if len(ps) > 1:
            raise Exception('{0} has more than one parent.  The parents are: {1}'.format(self,self.parents))
        elif len(ps) == 0:
            raise Exception('{0} has no parents'.format(self))
        else:
            return ps[0]
    @property
    def label(self):
        tags = '' if len(self.tags) == 0 else "\\n {0}".format("\\n".join(["{0}: {1}".format(re.sub('interval','chr',k),v) for k,v in self.tags.items() ]))
        return "[{3}] {0}{1}\\n{2}".format(self.__verbose__,tags,self.pcmd,self.id)
        
    @property
    def pcmd(self):
        return self.map_cmd()
    
    def map_cmd(self):
        try:
            inputs = [ p.get_output(self.inputs[0]) for p in self.parents ]
        except GetOutputError as e:
            raise GetOutputError("{0} tried to access a non-existant output file '{1}' in {2}".format(self,self.inputs[0],p))
        empty_parameter_values = [None] * len(inspect.getargspec(self.cmd)[0][2:]) #this will become the parameter config
        return self.middleware_cmd(inputs,*empty_parameter_values)
        
    def middleware_cmd(self,*args,**kwargs):
        """
        Stuff that happens inbetween map_cmd() and cmd()
        """
        decorated = pformat(self.cmd.im_func) #decorate with pformat
        decorated = types.MethodType(decorated,self,self.__class__) #rebind to self
        if 'params' in inspect.getargspec(decorated)[0]:
            r = decorated(params=self.parameters,*args,**kwargs)
        else:
            r = decorated(*args,**kwargs)
        m = re.search('\$OUT\.([\w]+)',r)
        if m:
            for out_name in m.groups():
                r = re.sub('\$OUT\.[\w]+',str(self.output_taskFiles[out_name]),r)
        return r
        
    def cmd(self,*args,**kwargs):
        raise NotImplementedError()
    
    def set_output_path(self,name,path):
        if name not in self.outputs:
            raise Exception('Invalid output name')
        self.output_paths[name] = path
    
    def __str__(self):
        return '[{0}] {1} {2}'.format(self.id,self.__class__.__name__,self.tags)
    
