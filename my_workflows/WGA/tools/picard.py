from cosmos.contrib.ezflow.tool import Tool
import os

class GATK(Tool):
    time_req = 5*60
    mem_req = 5*1024

    @property
    def bin(self):
        return 'java -Xmx{mem_req}m -Djava.io.tmpdir={s[tmp_dir]} -jar {s[GATK_path]}'.format(self=self,s=self.settings,mem_req=int(self.mem_req))
    
class Picard(Tool):
    time_req = 120
    mem_req = 2*1024

    @property
    def bin(self):
        return 'java -Xmx{mem_req}m -Djava.io.tmpdir={s[tmp_dir]} -jar {jar}'.format(self=self,mem_req=int(self.mem_req),s=self.settings,jar=os.path.join(self.settings['Picard_dir'],self.jar))


class FIXMATE(Picard):
    __verbose__ = "Fix Mate Information"
    inputs = ['bam']
    outputs = ['bam']
    one_parent = True
    time_req = 4*60
    mem_req = 5*1024

    jar = 'FixMateInformation.jar'

    def cmd(self,i,t,s,p):
        return r"""
            {self.bin}
            INPUT={i[bam]}
            OUTPUT=$OUT.bam
            VALIDATION_STRINGENCY=LENIENT
        """

class BAM2FASTQ(Picard):
    inputs = ['bam']
    outputs = ['dir']
    one_parent = True
    time_req = 4*60
    mem_req = 8*1024

    jar = 'SamToFastq.jar'

    def cmd(self,i,t,s,p):
        import re
        return r"""
            {self.bin}
            INPUT={i[bam]}
            OUTPUT_DIR=$OUT.dir
            OUTPUT_PER_RG=true
            VALIDATION_STRINGENCY=LENIENT
        """



class MERGE_SAMS(Picard):
    __verbose__ = "Merge Sam Files"
    mem_req = 3*1024
    inputs = ['sam']
    outputs = ['bam']
    default_params = { 'assume_sorted': False}
    
    jar = 'MergeSamFiles.jar'
    
    
    def cmd(self,i,t,s,p):
        return r"""
            {self.bin}
            {inputs}
            OUTPUT=$OUT.bam
            SORT_ORDER=coordinate
            MERGE_SEQUENCE_DICTIONARIES=True
            ASSUME_SORTED={p[assume_sorted]}
        """, {
        'inputs' : "\n".join(["INPUT={0}".format(n) for n in i['sam']]) 
        }
                
class CLEAN_SAM(Picard):
    __verbose__ = "Clean Sams"
    mem_req = 2*1024
    inputs = ['bam']
    outputs = ['bam']
    one_parent = True
    
    jar = 'CleanSam.jar'
    
    def cmd(self,i,t,s,p):
        return r"""
            {self.bin}
            I={i[bam]}
            O=$OUT.bam
        """

class DEDUPE(Picard):
    __verbose__ = "Mark Duplicates"
    mem_req = 5*1024
    inputs = ['bam']
    outputs = ['bam','metrics']
    one_parent = True
    
    jar = 'MarkDuplicates.jar'
    
    def cmd(self,i,t,s,p):
        return r"""
            {self.bin}
            I={i[bam]}
            O=$OUT.bam
            METRICS_FILE=$OUT.metrics
            ASSUME_SORTED=True
        """

class INDEX_BAM(Picard):
    __verbose__ = "Index Bam Files"
    mem_req = 2*1024
    forward_input = True
    inputs = ['bam']
    one_parent = True
    
    jar = 'BuildBamIndex.jar'
    
    def cmd(self,i,t,s,p):
        return r"""
            {self.bin}
            INPUT={i[bam]}
            OUTPUT={i[bam]}.bai
        """