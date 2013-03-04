#!/usr/bin/env python
"""
Split the output of Picard's RevertBam to Chunked Fastq Files
"""
import argparse
import os
import logging as log
import copy
from multiprocessing import Pipe,Process,log_to_stderr
from itertools import izip_longest
from subprocess import Popen, PIPE
import re

def grouper(n, iterable, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)


def samwriter(pipe,rgid_group,header_template,output_directory,samtools_path):
    output_files = {}
    for rgid in rgid_group:
        new_header = filter(lambda l:re.search('@RG.*\tID:{0}\t'.format(rgid),l) or not re.match('@RG',l),header_template) #filter out other readgroups

        out_path=os.path.join(output_directory,rgid+'.bam')
        if os.path.exists(out_path): os.unlink(out_path)
        p = Popen('{0} view -S -b -o {1} -'.format(samtools_path,out_path).split(' '),stdin=PIPE,stderr=PIPE)
        p.stdin.writelines(new_header)
        output_files[rgid] = p.stdin
        log.info('Created file {0}'.format(out_path))

    try:
        while True:
            readstr,rgid = pipe.recv()
            output_files[rgid].write(readstr)
    except KeyboardInterrupt,EOFError:
        for of in output_files.values():
            of.close()
        return True

def rg2rgid(readgroup):
    return re.search('\tID:(.+?)\t',readgroup).group(1)

def splitBam(input_filename,output_directory,num_cores,samtools_path):
    p = Popen('{0} view -H {1}'.format(samtools_path,input_filename).split(' '),stdout=PIPE,stderr=PIPE)
    header = p.stdout.readlines()
    view_proc = Popen('{0} view {1}'.format(samtools_path,input_filename).split(' '),stdout=PIPE,stderr=PIPE)

    rgid2pipe = {}
    rgids = map(rg2rgid,filter(lambda l:re.match('@RG',l),header))
    num_rgs = len(rgids)
    num_cores = min(num_cores,len(rgids))
    num_rgids_per_proc = int(float(num_rgs)/float(num_cores)) #divide and round up
    processes = []
    rg_groups = list(grouper(num_rgids_per_proc,rgids)) #group into sizes of num_rgs/num_cores
    rg_groups[-1] = filter(lambda x: x is not None, rg_groups[-1]) #remove any filler values

    for rgid_group in rg_groups:
        pipeout,pipein = Pipe(duplex=False)
        for rgid in rgid_group:
            rgid2pipe[rgid] = pipein
        new_header = copy.copy(header)
        
        p = Process(target=samwriter,kwargs={
            'pipe':pipeout,
            'rgid_group':rgid_group,
            'header_template':new_header,
            'output_directory':output_directory,
            'samtools_path':samtools_path})
        p.start()

    log.info('{0} Readgroups identified.'.format(len(rgids)))

    j=0
    try:
        for readstr in view_proc.stdout:
            j+=1
            if((j % 100000)==0):  log.info('Processed {0} reads.'.format(j))
            rgid = re.search('\tRG:Z:(.+?)\t',readstr).group(1)
            rgid2pipe[rgid].send((readstr,rgid))
    except KeyboardInterrupt:
        log.info('User pressed ctrl+c, shutting down.')
        
    for p in rgid2pipe.values():
        p.close()
    for p in processes:
        p.join()


if __name__ == '__main__':
    logger = log_to_stderr()
    log.basicConfig(level=log.INFO, format='%(asctime)s %(levelname)s %(processName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input_filename',type=str,help="Path to the bam file",required=True)
    parser.add_argument('-o','--output_directory',type=str,help="The directory to output to",required=True)
    parser.add_argument('-s','--samtools_path',type=str,help="Path to samtools binary",required=True)
    parser.add_argument('-n','--num_cores',type=int,help="Number of cores to use.",required=False,default=1)

    opts = parser.parse_args()
    kwargs = dict(opts._get_kwargs())
    splitBam(**kwargs)