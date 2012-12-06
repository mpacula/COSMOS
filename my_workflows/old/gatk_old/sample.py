import re,os,csv,itertools

#Get fastq_pair info
def groupby(iterable,fxn):
    """aggregates an iterable using a function"""
    return itertools.groupby(sorted(iterable,key=fxn),fxn)
def fastq2lane(filename):
    return re.search('L(\d+)',filename).group(1)
def fastq2partNumber(filename):
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
            for partNumber,fastqs_in_readgroup in groupby(fastqs_in_lane,fastq2partNumber):
                fqs = [f for f in fastqs_in_readgroup]
                yield (Fastq(filename=fqs[0],lane=lane,partNumber=partNumber,input_dir=self.input_path),
                       Fastq(filename=fqs[1],lane=lane,partNumber=partNumber,input_dir=self.input_path))
        
class Fastq:
    filename = None #filename of the first set of reads
    lane = None
    partNumber = None

    def __init__(self,filename,lane,partNumber,input_dir):
        self.filename=filename
        self.lane=lane
        self.partNumber = partNumber
        self.input_dir= input_dir
        
    @property
    def path(self):
        return os.path.join(self.input_dir,self.filename)
        
    def __str__(self):
        return "FastQ_Pair| r1: {0.r1}, r2: {0.r2}, lane: {0.lane}, partNumber: {0.partNumber}".format(self)   
        