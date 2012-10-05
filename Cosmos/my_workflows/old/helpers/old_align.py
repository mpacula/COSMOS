import os
import re
import logging as L
import itertools

L.basicConfig(level=L.DEBUG)
fastq_dir = '/2GATK/cai/fastq'

class Sample:
    def __init__(self,name):
        self.lanes = [] #tuples of FastQ read pairs
        self.name = name
        self.bams = []

class FastQ:
    def __init__(self,filename):
        m = re.match('(?P<sample_name>.+?)_R(?P<read_pair>\d)\.fq_lane(?P<lane>\d+?)',filename)
        self.sample_name = m.group('sample_name')
        self.read_pair = m.group('read_pair')
        self.lane = m.group('lane')
        self.filename = filename
        self.file_path = os.path.join(fastq_dir,filename) 
    def __str__(self):
        return "sample_name: %s | read_pair: %s | lane: %s" % (self.sample_name,self.read_pair,self.lane)

#get all fastq files.  assuming one fastq is one fastQ!
fastq_files = filter(lambda x: re.search('fq',x) != None,os.listdir(fastq_dir))
L.debug('fastq_files:\n%s'% "\n".join(fastq_files))

fastQs = [ FastQ(filename) for filename in fastq_files ]
def organize_fastQs_by_sample(fastQs):
    def aggr(iterable,fxn):
        return itertools.groupby(sorted(iterable,key=fxn),fxn)

    samples = []
    for sample_name,sample_fqs in aggr(fastQs,lambda l:l.sample_name): 
        sample = Sample(sample_name) 
        for lane_name,lane_fqs in aggr(sample_fqs,lambda fq:fq.lane):
            s = sorted(lane_fqs,key=lambda fq: fq.read_pair)
            sample.lanes.append(tuple(s))
        samples.append(sample)
    return samples

samples = organize_fastQs_by_sample(fastQs)

reference = '~/gatk/bundle/b37/human_g1k_v37.fasta'
out_dir = '/2GATK/cai/alignment'
bwa = '~/gatk/tools/bwa-0.6.2/bwa'
cmds = []
for sample in samples:
    for fastq1,fastq2 in sample.lanes:
        fastq1.sai = os.path.join(out_dir,fastq1.filename+'.sai')
        fastq2.sai = os.path.join(out_dir,fastq2.filename+'.sai')
        bam_path = os.path.join(out_dir,'{0}_lane{1}'.format(sample.name,fastq1.lane+'.bam'))
        sample.bams.append(bam_path)
        #cmds.append('{0} aln -f {3} {1} {2.file_path}'.format(bwa,reference,fastq1,fastq1.sai))
        #cmds.append('{0} aln -f {3} {1} {2.file_path}'.format(bwa,reference,fastq2,fastq2.sai))
        cmds.append( '{0} sampe -r "@RG\tID:{2.sample_name}\tLB:{2.sample_name}\tSM:{2.sample_name}\tPU:Lane_{2.lane}\tPL:ILLUMINA" -f {4} {1} {2.sai} {3.sai} {2.file_path} {3.file_path}'.format(bwa,reference,fastq1,fastq2,bam_path))
qsub = 'qsub -r y -cwd -b y -V -l h_vmem=15G,virtual_free=10G'

with open('align_cmds.txt','wb') as f:
    f.write("\n".join(cmds))

