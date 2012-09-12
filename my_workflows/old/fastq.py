#!/home2/erik/workspace/Cosmos/venv/bin/python
# -*- coding: utf-8 -*-
"""
Created on Mon Aug  6 16:04:33 2012

@author: erik

@EAS139:136:FC706VJ:2:2104:15343:197393 1:Y:18:ATCACG
EAS139	the unique instrument name
136	the run id
FC706VJ	the flowcell id
2	flowcell lane
2104	tile number within the flowcell lane
15343	'x'-coordinate of the cluster within the tile
197393	'y'-coordinate of the cluster within the tile
1	the member of a pair, 1 or 2 (paired-end or mate-pair reads only)
Y	Y if the read fails filter (read is bad), N otherwise
18	0 when none of the control bits are on, otherwise it is an even numbe

"""

import re
import sys
from argh import arg,ArghParser
import logging
logging.basicConfig(level=logging.INFO)
import os

class ShortRead:
    def __init__(self,lines):
        self.raw = lines
        header = re.split('[\:s]',lines[0])
        self.lane = header[3]
        
    def __str__(self):
        return '\n'.join(self.raw)


def yield_shortReads(f):
    lines = []
    for line in f:
        lines.append(line.strip())
        if len(lines) % 4 == 0:
            yield ShortRead(lines)
            lines = []

@arg('-i','--input_file', type=file, help='The fastq')
@arg('-o', '--output_dir', type=str, help='The output directory')
def split_fastq_by_lanes(args):
    """
    split fastq by the read lanes.  
    """
    open_fileHandlers = {}
    for numReads,read in enumerate(yield_shortReads(args.input_file)):
        input_filename = os.path.basename(args.input_file.name)
        if not read.lane in open_fileHandlers.keys():
            new_filename = re.sub('.fq|.fastq','_lane{0}.fastq'.format(read.lane),input_filename)
            logging.info('new filename set to {0}'.format(new_filename))
            output_filepath = os.path.join(args.output_dir,new_filename)
            logging.info('Creating outputfile {0}'.format(output_filepath))
            open_fileHandlers[read.lane] = file(output_filepath,'wb')
        if numReads!= 0 and numReads % 1000000 == 0:
            logging.info('Written {0} reads total'.format(numReads))
        print >> open_fileHandlers[read.lane], read
                
@arg('-f', type=file, help='The fastq')
def check_for_different_lanes(args):                
    lastRead = None
    for read in yield_shortReads(args.f):
        if lastRead == None:
            lastRead = read
        else:
            if lastRead.lane != read.lane:
                raise Exception("Different lanes! \n%s\n%s" % (lastRead,read))
            else:
                lastRead = read
    print "All reads have the same lane"
                     
if __name__=='__main__':
    parser = ArghParser()
    parser.add_commands([check_for_different_lanes,split_fastq_by_lanes])
    parser.dispatch()
