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
        
class Sample:   
    name = None
    input_path = None
    flowcell = None
    fastq_pairs = []
    
    def __init__(self,name,path):
        self.name = name
        self.input_path = path
    def __str__(self):
        return "Sample| name: {0.name}, flowcell: {0.flowcell}, input_path: {0.input_path}".format(self)    
        
class Fastq_Pair:
    r1 = None #filename of the first set of reads
    r2 = None
    lane = None
    readGroupNumber = None

    def __init__(self,r1,r2,lane,rgn):
        self.r1=r1
        self.r2=r2
        self.lane=lane
        self.readGroupNumber = rgn
        
    def __str__(self):
        return "FastQ_Pair| r1: {0.r1}, r2: {0.r2}, lane: {0.lane}, readGroupNumber: {0.readGroupNumber}".format(self)   
        
samples = []

#Get Sample Input Information
for pool_dir in os.listdir(input_dir):
    pool_dir = os.path.join(input_dir,pool_dir)
    for sample_dir in filter(lambda x: re.search('Sample',x),os.listdir(pool_dir)):
        sample = Sample(name=re.sub('Sample_','',sample_dir),path=os.path.join(pool_dir,sample_dir))
        samples.append(sample)
        with open(os.path.join(sample.input_path,'SampleSheet.csv'), 'rb') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=',')
            sample.flowcell = reader.next()['FCID']

#Gunzip fastqs
B_gunzip = WF.add_batch("gunzip")
cmd = 'find {0} -name *.gz'.format(input_dir)
p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
gzs = p.communicate()[0]
for gz in gzs.split("\n"):
    if gz != '':
        name = os.path.basename(gz)
        B_gunzip.add_node(name=name,pre_command='gunzip -v {0}'.format(gz))   
WF.run_batch(B_gunzip)
WF.wait(B_gunzip)
    
#Get fastq_pair info
def aggregate(iterable,fxn):
    """aggregates an iterable using a function"""
    return itertools.groupby(sorted(iterable,key=fxn),fxn)
def fastq2lane(filename):
    return re.search('L(\d+)',filename).group(1)
def fastq2readGroupNumber(filename):
    return re.search('_(\d+)\.f',filename).group(1)
for sample in samples[1:3]:
    all_fastqs = filter(lambda x:re.search('\.fastq|\.fq$',x),os.listdir(sample.input_path))
    for lane,fastqs in aggregate(all_fastqs,fastq2lane):
        for readGroupNumber,fastqs in aggregate(fastqs,fastq2readGroupNumber):
            fastqs = [f for f in fastqs]
            sample.fastq_pairs.append(Fastq_Pair(r1=fastqs[0],r2=fastqs[1],lane=lane,rgn=readGroupNumber))
    
B_bwa_aln = WF.add_batch("BWA Align")
for sample in samples[1:3]:
    print sample
    for fastq_pair in sample.fastq_pairs[1:2]:
        r1_fullpath = os.path.join(sample.input_path,fastq_pair.r1)
        fr2_fullpath = os.path.join(sample.input_path,fastq_pair.r2)
        print fastq_pair
        B_bwa_aln.add_node(name = fastq_pair.r1,
                           pre_command = commands.bwa_aln(fq=r1_fullpath,output_sai='{output_dir}/{outputs[sai]}'),
                           outputs = {'sai':'{0}.sai'.format(fastq_pair.r1)})
        
        
WF.run_batch(B_bwa_aln)
WF.wait(B_bwa_aln)
    
WF.finished()
