#!/usr/bin/env python
"""
Chunks a fastq file

TODO implement a producer/consumer pattern
"""

import re
import os
import logging as log
import gzip
from cosmos.Cosmos.helpers import confirm
from argh import arg,dispatch_command
from itertools import islice


@arg('-c','--chunksize',type=int,help='Number of reads per fastq chunk, default is 1 million')
@arg('-b','--buffersize',type=int,help='Number of reads to keep in RAM, default is 40k')
def main(input_fastq,output_dir,chunksize=4000000,buffersize=40000):
    """
    Chunks a large fastq file into smaller pieces.
    """
    chunk = 0
    log.info('Opening {0}'.format(input_fastq))

    if input_fastq.endswith('.gz'):
        infile = gzip.open(input_fastq)
    else:
        infile = open(input_fastq,'r')
    output_prefix = os.path.basename(input_fastq)
    output_prefix = re.search("(.+?)(_001)*\.(fastq|fq)(\.gz)*",output_prefix).group(1)

    #write chunks
    while True:
        chunk += 1
        log.info('Reading {0} lines from input.'.format(chunksize*4))
        data = list(islice(infile,chunksize*4)) # read chunksize*4 lines
        if len(data) == 0:
            log.info('Done')
            return

        new_filename = '{0}_{1:0>3}'.format(output_prefix,chunk)
        output_path = os.path.join(output_dir,new_filename+'.fastq.gz')

        if os.path.exists(output_path):
            if not confirm('{0} already exists!  Are you sure you want to overwrite the file?', timeout=0):
                return

        outfile = gzip.open(output_path,'wb')
        log.info('Writing {0} lines to {1}'.format(len(data),output_path))
        outfile.writelines(data)
        outfile.close()

    infile.close()

if __name__ == '__main__':
    log.basicConfig(level=log.INFO, format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    dispatch_command(main)


