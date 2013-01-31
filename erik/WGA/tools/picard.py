from cosmos.contrib.ezflow.tool import Tool
import os

class Picard(Tool):
    time_req = 120
    mem_req = 3*1024
    cpu_req=1
    extra_java_args=''

    @property
    def bin(self):
        return 'java{self.extra_java_args} -Xmx{mem_req}m -Djava.io.tmpdir={s[tmp_dir]} -Dsnappy.loader.verbosity=true -jar {jar}'.format(
            self=self,
            mem_req=int(self.mem_req*.8),
            s=self.settings,
            jar=os.path.join(self.settings['Picard_dir'],self.jar),
            )


class FIXMATE(Picard):
    __verbose__ = "Fix Mate Information"
    inputs = ['bam']
    outputs = ['bam']
    one_parent = True
    time_req = 4*60
    mem_req = 3*1024

    jar = 'FixMateInformation.jar'

    def cmd(self,i,s,p):
        return r"""
            {self.bin}
            INPUT={i[bam]}
            OUTPUT=$OUT.bam
            VALIDATION_STRINGENCY=LENIENT
        """


class REVERTSAM(Picard):
    inputs = ['bam']
    outputs = ['bam']
    one_parent = True
    time_req = 0
    mem_req = 20*1024
    cpu_req=2
    succeed_on_failure = False

    extra_java_args =' -XX:ParallelGCThreads={0}'.format(cpu_req+1)

    jar = 'RevertSam.jar'

    def cmd(self,i,s,p):
        return r"""
            {self.bin}
            INPUT={i[bam]}
            OUTPUT=$OUT.bam
            VALIDATION_STRINGENCY=SILENT
            MAX_RECORDS_IN_RAM=4000000
        """

class SAM2FASTQ_byrg(Picard):
    inputs = ['bam']
    outputs = ['dir']
    one_parent = True
    time_req = 0
    mem_req = 12*1024
    succeed_on_failure = True

    jar = 'SamToFastq.jar'

    def cmd(self,i,s,p):
        return r"""
            {self.bin}
            INPUT={i[bam]}
            OUTPUT_DIR=$OUT.dir
            OUTPUT_PER_RG=true
            VALIDATION_STRINGENCY=LENIENT
        """


class SAM2FASTQ(Picard):
    """
    Assumes sorted
    """
    inputs = ['bam']
    outputs = ['1.fastq','2.fastq']
    one_parent = True
    time_req = 0
    mem_req = 3*1024
    succeed_on_failure = True

    jar = 'SamToFastq.jar'

    def cmd(self,i,s,p):
        import re
        return r"""
            {self.bin}
            INPUT={i[bam]}
            FASTQ=$OUT.1.fastq
            SECOND_END_FASTQ=$OUT.2.fastq
            VALIDATION_STRINGENCY=SILENT
        """


class MERGE_SAMS(Picard):
    __verbose__ = "Merge Sam Files"
    mem_req = 3*1024
    inputs = ['bam']
    outputs = ['bam']
    default_params = { 'assume_sorted': False}
    
    jar = 'MergeSamFiles.jar'
    
    
    def cmd(self,i,s,p):
        return r"""
            {self.bin}
            {inputs}
            OUTPUT=$OUT.bam
            SORT_ORDER=coordinate
            MERGE_SEQUENCE_DICTIONARIES=True
            ASSUME_SORTED={p[assume_sorted]}
        """, {
        'inputs' : "\n".join(["INPUT={0}".format(n) for n in i['bam']])
        }
                
class CLEAN_SAM(Picard):
    __verbose__ = "Clean Sams"
    mem_req = 4*1024
    inputs = ['sam']
    outputs = ['bam']
    one_parent = True
    
    jar = 'CleanSam.jar'
    
    def cmd(self,i,s,p):
        return r"""
            {self.bin}
            I={i[sam]}
            O=$OUT.bam
            VALIDATION_STRINGENCY=SILENT
        """

class DEDUPE(Picard):
    __verbose__ = "Mark Duplicates"
    mem_req = 4*1024
    inputs = ['bam']
    outputs = ['bam','metrics']
    one_parent = True
    
    jar = 'MarkDuplicates.jar'
    
    def cmd(self,i,s,p):
        return r"""
            {self.bin}
            I={i[bam]}
            O=$OUT.bam
            METRICS_FILE=$OUT.metrics
            ASSUME_SORTED=True
        """

class INDEX_BAM(Picard):
    __verbose__ = "Index Bam Files"
    mem_req = 4*1024
    forward_input = True
    inputs = ['bam']
    one_parent = True
    
    jar = 'BuildBamIndex.jar'
    
    def cmd(self,i,s,p):
        return r"""
            {self.bin}
            INPUT={i[bam]}
            OUTPUT={i[bam]}.bai
        """