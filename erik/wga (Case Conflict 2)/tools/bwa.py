from cosmos.contrib.ezflow.tool import Tool
import os

class ALN(Tool):
    __verbose__ = "Reference Alignment"
    mem_req = 4*1024
    cpu_req = 2
    time_req = 100
    forward_input = True
    one_parent = True
    inputs = ['fastq.gz']
    outputs = ['sai']
    default_params = { 'q': 5 }
    
    def cmd(self,i,s,p):
        """
        Expects tags: lane, chunk, library, sample, platform, flowcell, pair
        """
        return '{s[bwa_path]} aln -q {p[q]} -t {self.cpu_req} {s[bwa_reference_fasta_path]} {i[fastq.gz]} > $OUT.sai'

class SAMPE(Tool):
    __verbose__ = "Paired End Mapping"
    mem_req = 5*1024
    cpu_req = 1
    time_req = 120
    inputs = ['fastq.gz','sai']
    outputs = ['sam']

    def cmd(self,i,s,p):
        """
        Expects tags: lane, chunk, library, sample, platform, flowcell, pair
        """
        return r"""
            {s[bwa_path]} sampe
            -f $OUT.sam
            -r "@RG\tID:{RG_ID}\tLB:{p[library]}\tSM:{p[sample]}\tPL:{p[platform]}"
            {s[bwa_reference_fasta_path]}
            {i[sai][0]}
            {i[sai][1]}
            {i[fastq.gz][0]}
            {i[fastq.gz][1]}
            """, {
            'RG_ID':'{0}.L{1}'.format(p['flowcell'],p['lane'])
        }

    