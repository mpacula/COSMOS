import re,os,csv,itertools,sys

#Get fastq_pair info
def groupby(iterable,fxn):
    """aggregates an iterable using a function"""
    return itertools.groupby(sorted(iterable,key=fxn),fxn)
def fastq2lane(filename):
    return re.search('L(\d+)',filename).group(1)
def fastq2chunk(filename):
    return re.search('_(\d+)\.f',filename).group(1)   
           
class Sample:   
    name = None
    input_path = None
    flowcell = None
    
    def __init__(self,name,input_path,flowcell):
        self.name = name
        self.input_path = input_path
        self.flowcell=flowcell
        
    def __str__(self):
        return "Sample| name: {0.name}, flowcell: {0.flowcell}, input_path: {0.input_path}".format(self)

    @staticmethod
    def createFromPath(path):
        filename = os.path.basename(path)
        with open(os.path.join(path,'SampleSheet.csv'), 'rb') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=',')
            rows = [ r for r in reader ]
            flowcell = rows[0]['FCID']
            flowcell2 = rows[1]['FCID']
            if flowcell != flowcell2 or len(rows) > 2:
                raise Exception('flowcell not as expected')
                
            return Sample(name=re.sub('Sample_','',filename),input_path=path,flowcell=flowcell)
        
    def yield_fastq_pairs(self):
        all_fastqs = filter(lambda x:re.search('\.fastq|\.fq$',x),os.listdir(self.input_path))
        for lane,fastqs_in_lane in groupby(all_fastqs,fastq2lane):
            for chunk,fastqs_in_readgroup in groupby(fastqs_in_lane,fastq2chunk):
                fqs = [f for f in fastqs_in_readgroup]
                yield (Fastq(filename=fqs[0],lane=lane,chunk=chunk,input_dir=self.input_path),
                       Fastq(filename=fqs[1],lane=lane,chunk=chunk,input_dir=self.input_path))
        
class Fastq:
    filename = None #filename of the first set of reads
    lane = None
    chunk = None

    def __init__(self,filename,lane,chunk,input_dir):
        self.filename=filename
        self.lane=lane
        self.chunk = chunk
        self.input_dir= input_dir
        
    @property
    def path(self):
        return os.path.join(self.input_dir,self.filename)
        
    def __str__(self):
        return "FastQ_Pair| r1: {0.r1}, r2: {0.r2}, lane: {0.lane}, chunk: {0.chunk}".format(self)

from argh import command
from argh.helpers import dispatch_command
import json

def yield_file_dicts(input_dir,depth):
    sample_dirs = []
    if depth == 1:
        sample_dirs = filter(lambda x: x!='.DS_Store',os.listdir(input_dir))  
    elif depth == 2:
        for pool_dir in os.listdir(input_dir):
            sample_dirs += [ os.path.join(pool_dir,sample_dir) for sample_dir in filter(lambda x: x!='.DS_Store',os.listdir(os.path.join(input_dir,pool_dir))) ]
            
    for sample_dir in sample_dirs:
        sample = Sample.createFromPath(os.path.join(input_dir,sample_dir))
        for fqp in sample.yield_fastq_pairs():
            for i,fq in enumerate(fqp):
                yield {'path':fq.path ,
                        'sample':sample.name,
                        'flowcell':sample.flowcell,
                        'lane': fq.lane,
                        'chunk': fq.chunk,
                        'pair': i,
                        'library': 'LIB-'+sample.name,
                        'platform':'ILLUMINA',
                        }

@command
def main(input_dir=None,depth=1):
    """
    :param depth: 2 if directories are separated by pool
    """
    return json.dumps([f for f in yield_file_dicts(input_dir,depth)],indent=4)

if __name__ == '__main__':    
    dispatch_command(main)
    
    
def yield_simulated_files(input_dir):
    for f in os.listdir(input_dir):
        path = os.path.join(input_dir,f)
        sample = re.search(r'(sim\d\d)',f).groups()[0]
        pair = re.search(r'R(\d)',f).groups()[0]
        yield {'path':path ,
                   'sample':sample,
                   'flowcell': 'noflow',
                   'lane': 'nolane',
                   'chunk': '1',
                   'pair': pair,
                   'library': 'LIB-'+sample,
                   'platform':'ILLUMINA',
                   }
    
        