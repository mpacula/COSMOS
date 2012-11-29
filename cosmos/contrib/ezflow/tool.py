from helpers import getcallargs,cosmos_format
import collections
import re,types,itertools
from cosmos.Workflow.models import TaskFile
from cosmos.Cosmos.helpers import parse_cmd

i = 0
def get_id():
    global i
    i +=1
    return i

files = []

#def return_inputs(*args,**kwargs):
#    r = kwargs.copy()
#    for arg in args:
#        if arg:
#            name = arg[0].name if type(arg) == list else arg.name
#            #TODO check for collisions
#            r[name] = arg
#    return r

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
    :property dag: The dag that is keeping track of this Tool
    :property id: A unique identifier.  Useful for debugging.
    :property output_files: This Tool's TaskFiles
    :property tags: This Tool's tags
    :property inputs: a list of input names, must be specified by user
    :property outputs: a list of output names, must be specified by user
    """
    NOOP = False #set to True if this Task should never actually be submitted to the Cosmos Workflow
    
    mem_req = 1*1024
    cpu_req = 1
    parameters = {}
    tags = {}
    inputs = []
    outputs = []
    forward_input = False
    
    def __init__(self,stage_name=None,tags={},dag=None):
        """
        :param stage_name: (str) The name of the stage this tool belongs to. Required.
        :param dag: the dag this task belongs to.
        """
        if not hasattr(self,'output_files'): self.output_files = {} 
        self.tags = tags
        self.stage_name = stage_name if stage_name else self.__class__.__name__
        if not hasattr(self,'__verbose__'): self.__verbose__ = self.stage_name
        self.dag = dag
        
        self.id = get_id()
        
        for output_ext in self.outputs:
            tf = TaskFile(fmt=output_ext)
            self.add_output(tf)
    
    def get_output(self,name):
        if self.forward_input and name not in self.output_files:
            return self.parent.get_output(name)
        
        try:
            return self.output_files[name]
        except KeyError:
            raise GetOutputError("{0} does not exist in {1}.  output_files available are {2}".format(name,self,self.output_files))
        
    def add_output(self,taskfile):
        if not isinstance(taskfile,TaskFile):
            raise ExpectedError('Expected a TaskFile')
        name = taskfile.name
        if name not in self.output_files:
            self.output_files[name] = []
        self.output_files[name].append(taskfile)
            
    def add_outputs(self,*taskfiles):
        if not isinstance(taskfiles[0],TaskFile):
            raise ExpectedError('Expected an iterable of TaskFiles')
        for taskfile in taskfiles:
            self.add_output(taskfile)
    
    
    @property
    def parents(self):
        return self.dag.G.predecessors(self)
    
    @property
    def parent(self):
        ps = self.dag.G.predecessors(self)
        if len(ps) > 1:
            raise Exception('{0} has more than one parent.  The parents are: {1}'.format(self,self.parents))
        elif len(ps) == 0:
            raise Exception('{0} has no parents'.format(self))
        else:
            return ps[0]
    @property
    def label(self):
        tags = '' if len(self.tags) == 0 else "\\n {0}".format("\\n".join(["{0}: {1}".format(re.sub('interval','chr',k),v) for k,v in self.tags.items() ]))
        return "\"[{3}] {0}{1}\\n{2}\"".format(self.__verbose__,tags,self.pcmd,self.id)
        
    @property
    def pcmd(self):
        if self.NOOP:
            return ''
        return self.process_cmd()

    def map_inputs(self):
        if not self.inputs:
            return {}
        input_files = { }
        try:
            for i in self.inputs:
                input_files[i] = map(lambda tf: str(tf),[ p.get_output(i) for p in self.parents ])
        except GetOutputError as e:
            raise GetOutputError("Error in {0}.  {1}".format(self,e))
        return input_files
        
    @property
    def input_files(self):
        try:
            r = self.map_inputs()
        except IndexError:
            raise
        return r
        
    def process_cmd(self):
        """
        Stuff that happens inbetween map_inputs() and cmd()
        :param *args: input file parameters
        :param **kwargs: Named input file parameters
        """
        #these arguments should be filled in later by decorators like from_tags
#        try:
        callargs = getcallargs(self.cmd,i=self.input_files,t=self.tags,s=self.settings,p=self.parameters)
        del callargs['self']
        r = self.cmd(**callargs)
        extra_format_dict = r[1] if len(r) == 2 else {}
        pcmd = r[0] if len(r) == 2 else r 
            
        m = re.search('\$OUT\.([\w]+)',pcmd)
#        except TypeError:
#            raise ToolError('TypeError calling {0}.cmd, args: {1}, kwargs: {2}.  Available args are: {3}'.format(self.__class__.__name__,args,kwargs,inspect.getargspec(decorated)[0]))
        if m:
            for out_name in m.groups():
                try:
                    pcmd = re.sub('\$OUT\.[\w]+',str(self.get_output(out_name)),pcmd)
                except KeyError as e:
                    raise KeyError('Invalid key in $OUT.key. Available output_file keys in {1} are {2}'.format(e,self,self.output_files.keys()))
                
        #format() return string with callargs
        callargs['self'] = self
        callargs.update(extra_format_dict)
        return parse_cmd(cosmos_format(pcmd,callargs))
        
    def cmd(self,*args,**kwargs):
        raise NotImplementedError("{0}.cmd is not implemented.".format(self.__class__.__name__))
    
    def __str__(self):
        return '[{0}] {1} {2}'.format(self.id,self.__class__.__name__,self.tags)
    
class INPUT(Tool):
    NOOP = True
    
    def __init__(self,*args,**kwargs):
        filepaths = kwargs.pop('filepaths')
        self.output_files = {}
        for fp in filepaths:
            tf = TaskFile(path=fp)
            self.add_output(tf)
        super(INPUT,self).__init__(*args,**kwargs)
    