import pysam
from subprocess import Popen,PIPE
from itertools import izip
import logging as log
import argparse
import os
import gzip

class FastqChunk:
    """
    A Chunked fastq
    """
    def __init__(self,output_directory,file_prefix,chunk_num):
        self.write_count = 0
        self.chunk_num = chunk_num
        filepath = os.path.join(output_directory,file_prefix+'_{0:0>3}.fq.gz'.format(chunk_num))
        #TODO check if file exists already
        self.file_handler = gzip.open(filepath,'wb')
        log.info('Created chunk {0}.'.format(self))

    def write(self,data):
        self.write_count+=1
        self.file_handler.write(data)

    def close(self):
        log.info('Closing chunk {0}.'.format(self))
        self.file_handler.close()

    def __str__(self):
        return self.samfile.filename

class ChunkManager:
    """
    A manager of Chunked files
    """
    def __init__(self,chunk_class,chunk_size,output_directory):
        self.chunk_size = chunk_size
        self.output_directory=output_directory
        self.chunks = {}

    def close(self):
        for chunk in self.chunks:
            chunk.close()

    def new_chunk(self,chunk_key):
        return self.chunk_class(output_directory=self.output_directory,
            file_prefix=chunk_key,
            chunk_num=len(self.chunks[chunk_key])+1
        )

    def get_chunk(self,chunk_key):
        try:
            chunk = self.chunks[chunk_key][-1]
            if chunk.writes >= self.chunk_size:
                chunk.close()
                self.chunks[chunk_key].append(self.new_chunk(chunk_key))
                chunk = self.chunks[chunk_key][-1]
        except KeyError:
            chunk = self.new_chunk(chunk_key)
            self.chunks[chunk_key] = [chunk]
        return chunk

    def write(self,chunk_key,data):
        """
        Writes a read.  Closes a chunk and creates a new one if necessary.
        """
        chunk = self.get_chunk(chunk_key)
        chunk.write(data)

def bam2fastq(input_filename,chunk_size,output_directory):
    CM = ChunkManager(chunk_class=FastqChunk,chunk_size=chunk_size,output_directory=output_directory)
    cmd = 'java -Xmx9830m -Djava.io.tmpdir=/mnt/tmp -jar /cosmos/WGA/tools/picard-tools-1.81/RevertSam.jar INPUT=/home2/erik/data.bam OUTPUT=/dev/stdout QUIET=true COMPRESSION_LEVEL=0 | /cosmos/WGA/tools/samtools-0.1.18/samtools view /dev/stdin'.split(" ")
    data_stream = Popen(cmd, stdout=PIPE)

    i=0
    for line in data_stream.stdout:
        next_line = data_stream.stdout.next()
        print line, next_line
        i+=1
        if i == 6:
            break

    CM.close()



if __name__ == '__main__':
    log.basicConfig(level=log.INFO, format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input_filename',type=str,help="Path to the bam file",required=True)
    parser.add_argument('-o','--output_directory',type=str,help="The directory to output to",required=True)
    parser.add_argument('-c','--chunk_size',type=int,help="The number of reads to chunk bams by.  The default is 0 which means do not chunk.",required=False,default=0)

    opts = parser.parse_args()
    bam2fastq(opts.input_filename,opts.output_directory,opts.chunk_size)