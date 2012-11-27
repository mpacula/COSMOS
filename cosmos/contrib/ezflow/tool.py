from decorators import pformat, getcallargs
import re
import types
import itertools
from cosmos.Workflow.models import TaskFile

i = 0
def get_id():
    global i
    i +=1
    return i

files = []

def return_inputs(*args,**kwargs):
    r = kwargs.copy()
    for arg in args:
        name = arg[0].name if type(arg) == list else arg.name
        #TODO check for collisions
        r[name] = arg
    return r

class ExpectedError(Exception): pass
class ToolError(Exception): pass
class GetOutputError(Exception): pass

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
    
    :property stage_name: (str) The name of this Tool's stage.  Defaults to the name of the class.
    :property DAG: The DAG that is keeping track of this Tool
    :property id: A unique identifier.  Useful for debugging.
    :property output_files: This Tool's TaskFiles
    :property tags: This Tool's tags
    :property inputs: a list of input names, must be specified by user
    :property outputs: a list of output names, must be specified by user
    """
    NOOP = False #set to True if this Task should never actually be submitted to the Cosmos Workflow
    
    mem_req = 1*1024
    cpu_req = 1
    params = {}
    tags = {}
    
    def __init__(self,DAG,stage_name,tags={},output_files=[]):
        """
        :param stage_name: (str) The name of the stage this tool belongs to. Required.
        :param output_files: A list, or a dict of TaskFiles.  if a list, it will be converted to a dict who's keywords are the TaskFiles' fmt. 
        
        """
        self.tags = tags
        self.stage_name = stage_name
        self.output_files = {}
        if not hasattr(self,'__verbose__'): self.__verbose__ = self.stage_name
        self.DAG = DAG
        
        self.id = get_id()
                
        for tf in output_files:
            #TODO check for collisions
            self.output_files[tf.name] = tf
            
        for output_ext in self.outputs:
            if output_ext not in self.output_files:
                tf = TaskFile(fmt=output_ext)
                #TODO check for collisions
                self.output_files[tf.name] = tf
        
    
    def get_output(self,name):
        try:
            return self.output_files[name]
        except KeyError:
            raise GetOutputError("{0} does not exist in {1}.output_files".format(name,self))
    
    
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
    def input_files_aslist(self):
        return itertools.chain(*self.input_files.values())
    
    @property
    def output_files_aslist(self):
        return itertools.chain(*self.output_files.values())   
        
    @property
    def pcmd(self):
        if self.NOOP:
            return ''
        return self.process_cmd()

    def map_inputs(self):
        try:
            inputs = [ p.get_output(self.inputs[0]) for p in self.parents ]
        except GetOutputError as e:
            raise GetOutputError("{0} tried to access a non-existant output file '{1}' in {2}".format(self,self.inputs[0],p))
        return return_inputs(inputs)
        
    @property
    def input_files(self):
        try:
            r = self.map_inputs()
        except IndexError:
            raise ToolError("map_inputs returned bad inputs, it returned {0}".format(r))
        return r
        
    def process_cmd(self):
        """
        Stuff that happens inbetween map_inputs() and cmd()
        :param *args: input file parameters
        :param **kwargs: Named input file parameters
        """
        decorated = pformat(self.cmd.im_func) #decorate with pformat
        decorated = types.MethodType(decorated,self,self.__class__) #rebind to self
        
        #these arguments should be filled in later by decorators like from_tags
#        try:
        r = decorated(self.input_files,self.tags,self.params)
        m = re.search('\$OUT\.([\w]+)',r)
#        except TypeError:
#            raise ToolError('TypeError calling {0}.cmd, args: {1}, kwargs: {2}.  Available args are: {3}'.format(self.__class__.__name__,args,kwargs,inspect.getargspec(decorated)[0]))
        if m:
            for out_name in m.groups():
                r = re.sub('\$OUT\.[\w]+',str(self.output_files[out_name]),r)
                #TODO memoization will occur here.  if this pcmd has already been executed,
                #set output_files to the memoized output path, and set this task as some kind of a NOOP
        return r
        
    def cmd(self,*args,**kwargs):
        raise NotImplementedError("{0}.cmd is not implemented.".format(self.__class__.__name__))
    
    def __str__(self):
        return '[{0}] {1} {2}'.format(self.id,self.__class__.__name__,self.tags)
    
