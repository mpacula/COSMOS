from helpers import getcallargs,cosmos_format
import re
from cosmos.Workflow.models import TaskFile
from cosmos.Cosmos.helpers import parse_cmd

i = 0
def get_id():
    global i
    i +=1
    return i

files = []

class ExpectedError(Exception): pass
class ToolError(Exception): pass
class GetOutputError(Exception): pass

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
    :property default_params: a dictionary of default parameters
    """
    NOOP = False #set to True if this Task should never actually be submitted to the Cosmos Workflow
    
    mem_req = 1*1024
    cpu_req = 1
    inputs = []
    outputs = []
    forward_input = False
    one_parent = False
    settings = {}
    parameters = {}
    tags = {}
    default_params = {}
    
    def __init__(self,stage_name=None,tags={},dag=None):
        """
        :param stage_name: (str) The name of the stage this tool belongs to. Required.
        :param dag: the dag this task belongs to.
        """
        if not hasattr(self,'output_files'): self.output_files = []
        self.tags = tags
        if not hasattr(self,'__verbose__'): self.__verbose__ = self.__class__.__name__
        self.stage_name = stage_name if stage_name else self.__verbose__
        self.dag = dag
        
        self.id = get_id()
        
        for output_ext in self.outputs:
            tf = TaskFile(fmt=output_ext)
            self.add_output(tf)
    
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
    
    def get_output(self,name):
        """
        Returns a list of output TaskFiles who's name == name.  If list is of size one, return the first element.
        
        :param name: the name of the output files.
        """
        outputs = filter(lambda x: x.name == name,self.output_files)
        if len(outputs) == 0:
            if self.forward_input:
                return self.parent.get_output(name)
            else:
                raise GetOutputError('No output file in {0} with name {1}'.format(self,name))
        return outputs if len(outputs) >1 else outputs[0]
    
    def get_output_file_names(self):
        return set(map(lambda x: x.name, self.output_files))
        
    def add_output(self,taskfile):
        """
        Adds an output file to this Task
        
        :param taskfile: an instance of a TaskFile
        """
        self.output_files.append(taskfile)
        
    @property
    def input_files(self):
        "A dictionary of input files"
        return self.map_inputs()
    
    @property
    def label(self):
        "Label used for the DAG image"
        tags = '' if len(self.tags) == 0 else "\\n {0}".format("\\n".join(["{0}: {1}".format(k,v) for k,v in self.tags.items() ]))
        return "[{3}] {0}{1}\\n{2}".format(self.__verbose__,tags,self.pcmd,self.id)

    def map_inputs(self):
        """
        Default method to map inputs.  Can be overriden of a different behavior is desired
        """
        if not self.inputs:
            return {}
        input_files = { }
        try:
            for i in self.inputs:
                input_files[i] = map(lambda tf: str(tf),[ p.get_output(i) for p in self.parents ])
                if self.one_parent:
                    for k in input_files:
                        if len(input_files[k]) == 1: input_files[k] = input_files[k][0]
        except GetOutputError as e:
            raise GetOutputError("Error in {0}.  {1}".format(self,e))
        return input_files
        
        
    @property
    def pcmd(self):
        return self.process_cmd() if not self.NOOP else ''
    
    def process_cmd(self):
        """
        Stuff that happens inbetween map_inputs() and cmd()
        :param *args: input file parameters
        :param **kwargs: Named input file parameters
        """
        callargs = getcallargs(self.cmd,i=self.input_files,t=self.tags,s=self.settings,p=self.parameters)
        del callargs['self']
        r = self.cmd(**callargs)
        
        #if tuple is returned, second element is a dict to format with
        extra_format_dict = r[1] if len(r) == 2 else {}
        pcmd = r[0] if len(r) == 2 else r 
        
        #replace $OUT with taskfile    
        for out_name in re.findall('\$OUT\.([\w]+)',pcmd):
            try:
                pcmd = pcmd.replace('$OUT.{0}'.format(out_name),str(self.get_output(out_name)))
            except KeyError as e:
                raise KeyError('Invalid key in $OUT.key. Available output_file keys in {1} are {2}'.format(e,self,self.get_output_file_names()))
                
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
    mem_req = 0
    cpu_req = 0
    
    def __init__(self,*args,**kwargs):
        filepaths = kwargs.pop('output_paths')
        super(INPUT,self).__init__(*args,**kwargs)
        for fp in filepaths:
            tf = TaskFile(path=fp)
            self.add_output(tf)
    