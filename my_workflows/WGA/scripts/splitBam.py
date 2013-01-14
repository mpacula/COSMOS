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
import copy

class Chunk:
    def __init__(self,output_directory,header,chunk_num):
        self.count = 0
        filepath = os.path.join(output_directory,header['RG'][0]['ID']+'_{0}.bam'.format(chunk_num))
        self.samfile = pysam.Samfile(filepath,'wb',header=header)

    def __str__(self):
        return self.sam.filename

class ChunkManager:
    def __init__(self,chunk_size,output_directory,header):
        self.chunk_size = chunk_size
        self.output_directory=output_directory
        self.header = header

        self.chunks = [ Chunk(output_directory,header,001) ]

    @property
    def current_chunk(self):
        return len(self.chunksize)

    def write(self,read):
        """
        Writes a read.  Closes a chunk and creates a new one if necessary.
        """
        chunk = self.chunks[-1]
        chunk.write(read)
        chunk.count += 1
        if chunk.count == self.chunk_size:
            log.info('Finished writing chunk {0}'.format(chunk))
            chunk.samfile.close()
            self.chunks.append(Chunk(self.output_directory,self.header,self.current_chunk))

def splitBam(input_filename,output_directory,chunk_size):
    infile = pysam.Samfile(input_filename,'rb')

    readgroups = {}
    for rg in infile.header['RG']:
        readgroups[rg['ID']] = rg

    #log.debug('BAM Header: \n{0}\n'.format(pprint.pformat(header,indent=2)))

    output_chunks = {} # rgid -> Chunk
    j=0
    for read in infile:
        j+=1
        if((j % 100000)==0):  log.info('Processed {0} reads.'.format(j))

        idtag = [x[1] for x in read.tags if x[0]=='RG'][0]

        if idtag not in output_chunks:
            filename = os.path.join(output_directory,rg['ID']+'.bam')
            log.info('Creating new BAM file: {0}'.format(filename))
            #generate header
            tmphead = copy.copy(infile.header)
            tmphead['RG'] = [readgroups[idtag]]
            #open file
            output_chunks[idtag] = Chunk(filename,tmphead)

        output_chunks[idtag].samfile.write(read)
        output_chunks[idtag].count += 1

        if chunk_size > 0 and output_chunks.count == chunk_size:

    log.info("Closing remaining {0} chunks".format(len(output_chunks)))
    for chunk in output_chunks.values():
        chunk.samfile.close()

    infile.close()

if __name__ == '__main__':
    log.basicConfig(level=log.INFO, format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input_filename',type=str,help="Path to the bam file",required=True)
    parser.add_argument('-o','--output_directory',type=str,help="The directory to output to",required=True)
    parser.add_argument('-c','--chunk_size',type=int,help="The number of reads to chunk bams by.  The default is 0 which means do not chunk.",required=False,default=0)

    opts = parser.parse_args()
    splitBam(opts.input_filename,opts.output_directory,opts.chunk_size)