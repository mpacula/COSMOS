#Import Cosmos
import sys,os,re
import itertools
import csv
import cosmos_session
from Workflow.models import Workflow, Batch
from Cosmos.helpers import parse_command_string
import commands

import subprocess
WF = Workflow.resume(name='GPP_48Exomes_GATK',dry_run=False)
assert isinstance(WF, Workflow)

input_dir = '/nas/erik/48exomes'

#Get fastq_pair info
def aggregate(iterable,fxn):
    """aggregates an iterable using a function"""
    return itertools.groupby(sorted(iterable,key=fxn),fxn)
def fastq2lane(filename):
    return re.search('L(\d+)',filename).group(1)
def fastq2readGroupNumber(filename):
    return re.search('_(\d+)\.f',filename).group(1)   
           
class Sample:   
    name = None
    input_path = None
    flowcell = None
    fastq_pairs = []
    
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
            flowcell = reader.next()['FCID']
            return Sample(name=re.sub('Sample_','',filename),input_path=path,flowcell=flowcell)
         
        
    def yield_fastq_pairs(self):
        fastq_pairs = []
        all_fastqs = filter(lambda x:re.search('\.fastq|\.fq$',x),os.listdir(self.input_path))
        for lane,fastqs_in_lane in aggregate(all_fastqs,fastq2lane):
            for readGroupNumber,fastqs_in_readgroup in aggregate(fastqs_in_lane,fastq2readGroupNumber):
                fqs = [f for f in fastqs_in_readgroup]
                yield Fastq_Pair(r1=fqs[0],r2=fqs[1],lane=lane,rgn=readGroupNumber,input_dir=self.input_path)
        
class Fastq_Pair:
    r1 = None #filename of the first set of reads
    r2 = None
    lane = None
    readGroupNumber = None

    def __init__(self,r1,r2,lane,rgn,input_dir):
        self.r1=r1
        self.r2=r2
        self.lane=lane
        self.readGroupNumber = rgn
        self.input_dir= input_dir
        
    @property
    def r1_path(self):
        return os.path.join(self.input_dir,self.r1)
    @property
    def r2_path(self):
        return os.path.join(self.input_dir,self.r2)
        
    def __str__(self):
        return "FastQ_Pair| r1: {0.r1}, r2: {0.r2}, lane: {0.lane}, readGroupNumber: {0.readGroupNumber}".format(self)   
        
samples = []

#Get Sample Input Information
for pool_dir in os.listdir(input_dir):
    for sample_dir in os.listdir(os.path.join(input_dir,pool_dir)):
        if re.match('Sample',sample_dir):
            samples.append(Sample.createFromPath(os.path.join(input_dir,pool_dir,sample_dir)))

#Gunzip fastqs
B_gunzip = WF.add_batch("gunzip")
cmd = 'find {0} -name *.gz'.format(input_dir)
p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
gzs = p.communicate()[0]
for gz in gzs.split("\n"):
    if gz != '':
        name = os.path.basename(gz)
        B_gunzip.add_node(name=name,pcmd='gunzip -v {0}'.format(gz))   
WF.run_batch(B_gunzip)
WF.wait(B_gunzip)



B_bwa_aln = WF.add_batch("BWA Align",hard_reset=True)
for sample in samples:
    print sample
    for fqp in sample.yield_fastq_pairs():
        node = B_bwa_aln.add_node(name = fqp.r1,
                           pcmd = commands.bwa_aln(fq=fqp.r1_path,output_sai='{output_dir}/{outputs[sai]}'),
                           outputs = {'sai':'{0}.sai'.format(fqp.r1)})
        fqp.r1_sai_node = node
        node = B_bwa_aln.add_node(name = fqp.r2,
                           pcmd = commands.bwa_aln(fq=fqp.r2_path,output_sai='{output_dir}/{outputs[sai]}'),
                           outputs = {'sai':'{0}.sai'.format(fqp.r1)})
        fqp.r2_sai_node = node
        
        
WF.run_batch(B_bwa_aln)
WF.wait(B_bwa_aln)
    
WF.finished()
