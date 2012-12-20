from cosmos.contrib.ezflow.tool import Tool
import os

class ALN(Tool):
    __verbose__ = "Reference Alignment"
    mem_req = 3.5*1024
    cpu_req = 1 #2
    time_req = 100
    forward_input = True
    one_parent = True
    inputs = ['fastq']
    outputs = ['sai']
    default_params = { 'q': 5 }
    
    def cmd(self,i,t,s,p):
        return '{s[bwa_path]} aln -q {p[q]} -t {self.cpu_req} {s[bwa_reference_fasta_path]} {i[fastq]} > $OUT.sai'

class SAMPE(Tool):
    __verbose__ = "Paired End Mapping"
    mem_req = 5*1024
    cpu_req = 1 #4
    time_req = 120
    inputs = ['fastq','sai']
    outputs = ['sam']

    def cmd(self,i,t,s,p):
        t2 = self.parents[0].tags
        return r"""
            {s[bwa_path]} sampe
            -f $OUT.sam
            -r "@RG\tID:{RG_ID}\tLB:{t2[library]}\tSM:{t2[sample]}\tPL:{t2[platform]}"
            {s[bwa_reference_fasta_path]}
            {i[sai][0]}
            {i[sai][1]}
            {i[fastq][0]}
            {i[fastq][1]}
            """, {
            't2' : t2,
            'RG_ID':'{0}.L{1}'.format(t2['flowcell'],t2['lane'])
        }

    