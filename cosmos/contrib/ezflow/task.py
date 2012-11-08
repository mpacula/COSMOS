i = 0
def get_id():
    global i
    i +=1
    return i

class Input(object):

    def __init__(self, file_type):
        self.file_type = file_type

    def __call__(self, f):
        def wrapped_f(input_nodes,*args,**kwargs):
            input_nodes.outs[self.format]
            kwargs[self.file_type] = input_nodes
            f(*args,**kwargs)
        return wrapped_f

class Task:
    tags = {}
    def map_cmd(self,input_nodes):
        self.id
        
    def cmd(self,*args,**kwargs):
        raise NotImplementedError()
    
    def __init__(self,tags={}):
        self.id = get_id()
        self.tags = tags
    def __str__(self):
        return '[{0}] {1} {2}'.format(self.id,self.__class__.__name__,self.tags)


class ALN(Task):
    inputs = ['fastq']
    outputs = ['fastq','sai']
    @input('fastq')
    def cmd(self,fastq):
        return 'bwa aln $IN.fastq > $OUT.sai'.format(fastq)
class SAMPE(Task):
    outputs = ['sam']
    def map_cmd(self,input_nodes):
        "Expecting 2 input nodes"
        ins = input_nodes
        return self.cmd(ins[0].out['fastq'],ins[1].out['fastq'],ins[0].out['sai'],ins[1].out['sai'])
    def cmd(self,fastq1,fastq2,aln1,aln2):
        return 'bwa sampe {0} {1} {2} {3} > $OUT.sai'.format(fastq1,fastq2,aln1,aln2)
class CLEAN_SAM(Task):
    outputs=['bam']
    @input('sam')
    def cmd(self,sam):
        return ''.format()
class IRTC(Task):
    def cmd(self,input_bam):
        return 'IRTC -L {interval}'.format(interval = self.tags['interval']) 
class IR(Task):
    def cmd(self,input_bam):
        return 'IR -L {interval}'.format(interval = self.tags['interval']) 
class UG(Task):
    def cmd(self,input_file,glm,interval):
        return 'UnifiedGenotyper -I {0} -glm {1} -L {2}'.format(input_file,glm,interval)
class CV(Task):
    def cmd(self,input_bams):
        inputs = '-I '.join(input_bams)
        return 'CombineVariants {inputs}'.format(inputs)
