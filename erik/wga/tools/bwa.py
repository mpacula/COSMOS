from cosmos.contrib.ezflow.tool import Tool
import os

class ALN(Tool):
    name = "Reference Alignment"
    mem_req = 4*1024
    cpu_req = 2
    time_req = 100
    forward_input = True
    inputs = ['fastq.gz']
    outputs = ['sai']
    default_params = { 'q': 5 }
    
    def cmd(self,i,s,p):
        """
        Expects tags: lane, chunk, library, sample, platform, flowcell, pair
        """
        return '{s[bwa_path]} aln -q {p[q]} -t {self.cpu_req} {s[bwa_reference_fasta_path]} {i[fastq.gz][0]} > $OUT.sai'

class SAMPE(Tool):
    name = "Paired End Mapping"
    mem_req = 5*1024
    cpu_req = 1
    time_req = 120
    inputs = ['fastq.gz','sai']
    outputs = ['sam']

    def cmd(self,i,s,p):
        """
        Expects tags: lane, chunk, library, sample, platform, flowcell, pair
        """
        #todo assert correct fastq and sai are paired
        t2 = self.parents[0].tags
        return r"""
            {s[bwa_path]} sampe
            -f $OUT.sam
            -r "@RG\tID:{RG_ID}\tLB:{t2[library]}\tSM:{t2[sample]}\tPL:{t2[platform]}"
            {s[bwa_reference_fasta_path]}
            {i[sai][0]}
            {i[sai][1]}
            {i[fastq.gz][0]}
            {i[fastq.gz][1]}
            """, {
            't2' : t2,
            'RG_ID':'{0}.L{1}'.format(t2['flowcell'],t2['lane'])
        }

    