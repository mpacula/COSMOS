import pysam
from subprocess import Popen,PIPE

data_stream = Popen('/cosmos/WGA/tools/samtools-0.1.18/samtools view /cosmos/output/Bam_Chunk/REVERTSAM/196/out/out.bam', stdout=PIPE)
for i,line in enumerate(data_stream.stdout):
    print i
    print line
    if i ==5:
        break


header = { 'HD': {'VN': '1.0'},
           'SQ': [] }

outfile = pysam.Samfile('/tmp/empty.bam', "wh", header = header )

infile = pysam.Samfile('/cosmos/output/Bam_Chunk/REVERTSAM/196/out/out.bam',mode='rb')
for read in infile:
    import ipdb; ipdb.set_trace()
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