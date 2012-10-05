from sample import Sample,Fastq
import os,re

input_dir='/nas/erik/test_data3'
samples=[]

for sample_dir in os.listdir(input_dir):
    samples.append(Sample.createFromPath(os.path.join(input_dir,sample_dir)))
