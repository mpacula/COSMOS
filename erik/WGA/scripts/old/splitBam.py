#!/usr/bin/env python
"""
Split the output of Picard's RevertBam to Chunked Fastq Files
"""
import pysam
import argparse
import os
import logging as log
import copy
import multiprocessing

def splitBam(input_filename,output_directory,chunk_size):
    multiprocessing.Pool(2)

    infile = pysam.Samfile(input_filename,'rb')

    output_files = {}
    for rg in infile.header['RG']:
        new_header = copy.copy(infile.header)
        new_header['RG'] = [rg]
        out_path=os.path.join(output_directory,rg['ID']+'.bam')
        output_files[rg['ID']] = pysam.Samfile(out_path,'wb',header=new_header)

    log.info('{0} Readgroups identified.'.format(len(output_files)))

    j=0
    for read in infile:
        j+=1
        if((j % 100000)==0):  log.info('Processed {0} reads.'.format(j))

        idtag = [x[1] for x in read.tags if x[0]=='RG'][0]
        output_files[idtag].write(read)

    log.info("Closing {0} files".format(len(output_files)))
    for f in output_files.values():
        f.close()

    infile.close()

if __name__ == '__main__':
    log.basicConfig(level=log.INFO, format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    parser = argparse.ArgumentParser()
    parser.add_argument('-i','--input_filename',type=str,help="Path to the bam file",required=True)
    parser.add_argument('-o','--output_directory',type=str,help="The directory to output to",required=True)
    parser.add_argument('-c','--chunk_size',type=int,help="The number of reads to chunk bams by.  The default is 0 which means do not chunk.",required=False,default=0)

    opts = parser.parse_args()
    splitBam(opts.input_filename,opts.output_directory,opts.chunk_size)

"""
infile = pysam.Samfile('./NA12878.HiSeq.WGS.bwa.cleaned.recal.hg19.20.bam')
outfile = pysam.Samfile('test.bam','wb',header=infile.header)
j=0
for read in infile:
    outfile.write(read)
    if j > 1000:
        break
    j +=1

testfile = pysam.Samfile('test.bam')
pysam.sort(testfile,"-n")

"""

"""
import pysam

infile = pysam.Samfile('~/tmp/data/test_sorted.bam')
for read in infile:
    mate = None

    pointer = infile.tell() # pointer to the current position in the BAM file
    try:
        mate = infile.mate(read)
    except ValueError:
        print 'mate missing'
        infile.seek(pointer) # Return the BAM file to the position of read1 in the pair
        continue
    finally:
        'print rp'
        infile.seek(pointer) # Return the BAM file to the position of read1 in the pair
    if mate:
        print read
        print mate
        break

"""


f = pysam.Samfile('test.bam') # Open the SAM/BAM. Pysam should be able to guess the format and open it as a read-only file

for read in f: # Iterate over each read in the BAM.

    if read.is_proper_pair and read.is_read1 and read.tid == read.rnext and not read.is_duplicate: # Check the read satisfies a few QC conditions and is the first read of a pair (read1). All read2's are skipped over.

        chrom = f.getrname(read.tid) # Get the chromosome name. "read.tid" by itself is a unique code for the chromosome but wrapping it in 'f.getrname()' makes the chromosome id into a more palatable form, e.g. chr1, chr2, etc.


        # Try to find the mate (read2) of the read-pair, if it can't be found skip this read. Return pointer to position of read1 in the BAM file

        try:

            mate = f.mate(read)

        except ValueError:

        # Invalid mate (usually post-filtered)

            continue

        finally:

            f.seek(pointer) # Return the BAM file to the position of read1 in the pair

BAM = pysam.Samfile('NA12878.HiSeq.WGS.bwa.cleaned.recal.hg19.20.bam') # Open the SAM/BAM.Pysam should be able to guess the format and open it as a read-only file
