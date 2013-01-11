#!/usr/bin/env python
# split a bam file by read group ID
# Sean Davis <seandavi@gmail.com>
# March 10, 2012
#
# Modified by Erik Gafni
#
import pysam
import argparse
import os
import logging as log
import pprint

def splitBam(input_filename,output_directory):
    infile = pysam.Samfile(input_filename,'rb')

    header = infile.header
    log.info('BAM Header: \n{0}\n'.format(pprint.pformat(header,indent=2)))

    readgroups = header['RG']
    # remove readgroups from header
    del(header['RG'])

    outfiles = {}
    for rg in readgroups:
        tmphead = header
        tmphead['RG']=[rg]
        log.info('Creating new BAM file: %s',(rg['ID']+'.bam','wb'))
        outfiles[rg['ID']] = pysam.Samfile(os.path.join(output_directory,rg['ID']+'.bam'),'wb',header=tmphead)

    j=0
    for read in infile:
        j+=1
        idtag = [x[1] for x in read.tags if x[0]=='RG'][0]
        if((j % 100000)==0):
            log.info('read and wrote %d reads',(j))
        outfiles[idtag].write(read)

    for f in outfiles.values():
        f.close()

    infile.close()

if __name__ == '__main__':
    log.basicConfig(level=log.INFO, format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input_filename',type=str,help="Path to the bam file",required=True)
    parser.add_argument('-o','--output_directory',type=str,help="The directory to output to",required=True)

    opts = parser.parse_args()
    splitBam(opts.input_filename,opts.output_directory)