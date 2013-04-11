from helpers import getcallargs,cosmos_format
import re
from cosmos.Workflow.models import TaskFile
from cosmos.utils.helpers import parse_cmd

i = 0
def get_id():
    global i
    i +=1
    return i

files = []

class ExpectedError(Exception): pass
class ToolError(Exception): pass
class ToolValidationError(Exception):pass
class GetOutputError(Exception): pass

class Tool(object):
    """
    A Tool is a class who's instances represent a command that gets executed.  It also contains properties which
    define the resources that are required

    :property inputs: (list of strs) a list of input names. Defaults to [].  Can be overridden.
    :property outputs: (list of strs) a list of output names. Defaults to []. Can be overridden.
    :property mem_req: (int) Number of megabytes of memory to request.  Defaults to 1024.  Can be overridden.
    :property cpu_req: (int) Number of cores to request.  Defaults to 1.  Can be overridden.
    :property NOOP: (bool) If True, these tasks do not contain commands that are executed.  Used for INPUT.  Default is False. Can be overridden.
    :property forward_input: (bool) If True, the input files of this tool will also be input files of children of this tool.  Default is False.  Can be overridden.
    :property succeed_on_failure: (bool) If True, if this tool's tasks' job attempts fail, the task will still be considered successful.  Default is False.  Can be overridden.
    :property dont_delete_output_files: (bool) If True, this tool's output files will not be deleted when a workflow is executed with delete_intermediates=True.  Default is False.  Can be overridden.
    :property default_params: (dict) A dictionary of default parameters.  Defaults to {}.  Can be overridden.
    :property stage_name: (str) The name of this Tool's stage.  Defaults to the name of the class.
    :property dag: (DAG) The dag that is keeping track of this Tool
    :property id: (int) A unique identifier.  Useful for debugging.
    :property input_files: (list) This Tool's input TaskFiles
    :property output_files: (list) This Tool's output TaskFiles.  A tool's output taskfile names should be unique.
    :property tags: (dict) This Tool's tags.
    """
    #TODO props that cant be overridden should be private

    inputs = []
    outputs = []
    mem_req = 1*1024 #(MB)
    cpu_req = 1 #(cores)
    time_req = None #(mins)
    NOOP = False
    forward_input = False # If True, input of this tool can be accessed by its output files
    succeed_on_failure = False
    dont_delete_output_files = False
    default_params = {}

    settings = {}
    parameters = {}
    tags = {}
    
    def __init__(self,stage_name=None,tags={},dag=None):
        """
        :param stage_name: (str) The name of the stage this tool belongs to. Required.
        :param dag: the dag this task belongs to.
        """
        #if len(tags)==0: raise ToolValidationError('Empty tag dictionary.  All tools should have at least one tag.')

        if not hasattr(self,'output_files'): self.output_files = []
        if not hasattr(self,'name'): self.name = self.__class__.__name__
        self.tags = tags
        self.stage_name = stage_name if stage_name else self.name
        self.dag = dag
        
        self.id = get_id()

        # Create empty output TaskFiles
        for output_ext in self.outputs:
            tf = TaskFile(fmt=output_ext)
            self.add_output(tf)

        if len(self.inputs) != len(set(self.inputs)):
            raise ToolValidationError('Duplicate input names detected.  Perhaps try using [1.ext,2.ext,...]')

        if len(self.outputs) != len(set(self.outputs)):
            raise ToolValidationError('Duplicate output names detected.  Perhaps try using [1.ext,2.ext,...]')

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
        Returns the output TaskFiles who's name == name.  This should always be one element.
        
        :param name: the name of the output file.
        """

        outputs = filter(lambda x: x.name == name,self.output_files)

        if len(outputs) > 1: raise GetOutputError('More than one output with name {0} in {1}'.format(name,self))

        if len(outputs) == 0 and self.forward_input:
            try:
                outputs +=  [ self.parent.get_output(name) ]
            except GetOutputError as e:
                pass

        if len(outputs) == 0:
            raise GetOutputError('No output file in {0} with name {1}.'.format(self,name))

        return outputs[0]
    
    def get_output_file_names(self):
        return set(map(lambda x: x.name, self.output_files))
        
    def add_output(self,taskfile):
        """
        Adds an taskfile to self.output_files
        
        :param taskfile: an instance of a TaskFile
        """
        self.output_files.append(taskfile)
        
    @property
    def input_files(self):
        "An alias to :py:meth:map_inputs"
        return self.map_inputs()
    
    @property
    def label(self):
        "Label used for the DAG image"
        tags = '' if len(self.tags) == 0 else "\\n {0}".format("\\n".join(["{0}: {1}".format(k,v) for k,v in self.tags.items() ]))
        return "[{3}] {0}{1}\\n{2}".format(self.name,tags,self.pcmd,self.id)

    def map_inputs(self):
        """
        Default method to map inputs.  Can be overriden of a different behavior is desired
        :returns: (dict) A dictionary of taskfiles which are inputs to this tool.  Keys are names of the taskfiles, values are a list of taskfiles.
        """
        if not self.inputs:
            return {}

        elif '*' in self.inputs:
            return [ o for p in self.parents for o in p.output_files ]

        else:
            all_inputs = []
            for name in self.inputs:
                for p in self.parents:
                    all_inputs += [ p.get_output(name) ]

            input_dict = {}
            for input_file in all_inputs:
                input_dict.setdefault(input_file.name,[]).append(input_file)

            return input_dict
        
        
    @property
    def pcmd(self):
        return self.process_cmd() if not self.NOOP else ''
    
    def process_cmd(self):
        """
        Stuff that happens in between map_inputs() and cmd()
        """
        p = self.parameters.copy()
        p.update(self.tags)
        callargs = getcallargs(self.cmd,i=self.input_files,s=self.settings,p=p)
        del callargs['self']
        r = self.cmd(**callargs)
        
        #if tuple is returned, second element is a dict to format with
        extra_format_dict = r[1] if len(r) == 2 and r else {}
        pcmd = r[0] if len(r) == 2 else r 
        
        #replace $OUT with taskfile    
        for out_name in re.findall('\$OUT\.([\.\w]+)',pcmd):
            try:
                pcmd = pcmd.replace('$OUT.{0}'.format(out_name),str(self.get_output(out_name)))
            except KeyError as e:
                raise KeyError('Invalid key in $OUT.key. Available output_file keys in {1} are {2}'.format(e,self,self.get_output_file_names()))
                
        #format() return string with callargs
        callargs['self'] = self
        callargs.update(extra_format_dict)
        return parse_cmd(cosmos_format(pcmd,callargs))


    def cmd(self, i, s, p):
        """
        Constructs the preformatted command string.  The string will be .format()ed with the i,s,p dictionaries,
        and later, $OUT.outname  will be replaced with a TaskFile associated with the output name `outname`

        :param i: (dict) Input TaskFiles.
        :param s: (dict) Settings.  The settings dictionary, set by using :py:meth:`contrib.ezflow.dag.configure`
        :param p: (dict) Parameters.
        :returns: (str|tuple(str,dict)) A preformatted command string, or a tuple of the former and a dict with extra values to use for
            formatting
        """
        raise NotImplementedError("{0}.cmd is not implemented.".format(self.__class__.__name__))

    def __str__(self):
        return '[{0}] {1} {2}'.format(self.id,self.__class__.__name__,self.tags)
    
class INPUT(Tool):
    """
    An Input File.

    Does not actually execute anything, but provides a way to load an input file.

    >>> INPUT('/path/to/file.ext',tags={'key':'val'})
    >>> INPUT(path='/path/to/file.ext.gz',name='ext',fmt='ext.gz',tags={'key':'val'})
    """
    name = "Load Input Files"
    NOOP = True
    mem_req = 0
    cpu_req = 0
    
    def __init__(self,path,name=None,fmt=None,*args,**kwargs):
        """
        :param path: the path to the input file
        :param name: the name or keyword for the input file
        :param fmt: the format of the input file
        """
        super(INPUT,self).__init__(*args,**kwargs)
        self.add_output(TaskFile(path=path,name=name,fmt=fmt))

    def __str__(self):
        return '[{0}] {1} {2}'.format(self.id,self.__class__.__name__,self.tags)

    