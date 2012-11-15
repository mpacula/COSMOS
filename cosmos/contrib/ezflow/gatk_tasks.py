from task import Task
from decorators import pformat, fromtags, opoi

def list2input(l):
    return " -I ".join(l)

class FASTQ(Task):
    __verbose__ = "Fastq Input"
    
    outputs = ['fastq']
    
    def map_cmd(self):
        return None

class ALN(Task):
    __verbose__ = "Reference Alignment"
    inputs = ['fastq']
    outputs = ['sai']
    
    @opoi
    def cmd(self,fastq):
        return 'bwa aln {fastq} > $OUT.sai'
    
class SAMPE(Task):
    __verbose__ = "Paired End Mapping"
    
    inputs = ['sam']
    outputs = ['bam']
    
    def map_cmd(self):
        "Expecting 2 input nodes"
        ps = self.parents
        return self.middle_cmd(ps[0].get_output('fastq'),ps[1].get_output('fastq'),ps[0].get_output('sai'),ps[1].get_output('sai'))
    
    def cmd(self,fastq1,fastq2,aln1,aln2,P):
        return 'bwa sampe {fastq1} {fastq2} {aln1} {aln2} -q {P[q]} > $OUT.sam'
    
class CLEAN_SAM(Task):
    __verbose__ = "Sam Cleaning"
    
    inputs = ['sam']
    outputs=['bam']
    
    def cmd(self,sams):
        return 'cleansam {0} -o $OUT.bam'.format(list2input(sams))
    
class IRTC(Task):
    __verbose__ = "Indel Realigner Target Creator"
    call_cmd = ""
    
    inputs = ['bam']
    outputs = ['targets']
    
    @fromtags('interval')
    def cmd(self,input_bams,interval):
        return 'IRTC -I {0} -L {{interval}} > $OUT.targets'.format(list2input(input_bams))
    
class IR(Task):
    __verbose__ = "Indel Realigner"
    inputs = ['bam','targets']
    outputs = ['bam']
    
    def map_cmd(self):
        input_bam = self.parent.parent.get_output('bam')
        return self.middle_cmd(input_bam,self.parent.get_output('targets'),interval=self.tags['interval'])
    
    def cmd(self,input_bam,targets,interval):
        return 'IR -I {input_bam} -L {interval} -t {targets}'
    
class BQSR(Task):
    __verbose__ = "Base Quality Score Recalibration"
    inputs = ['bam']
    outputs = ['recal']
    
    def cmd(self,input_bams):
        return 'BQSR -I {0} > $OUT.recal'.format(' -I '.join(input_bams))
    
class PR(Task):
    __verbose__ = "Apply BQSR"
    inputs = ['bam','recal']
    outputs = ['bam']
    
    def map_cmd(self):
        input_bams = [p.get_output('bam') for p in self.parent.parents ] 
        return self.middle_cmd(input_bams,self.parent.get_output('recal'))
    
    def cmd(self,input_bams,recal):
        return 'PrintReads -I {0} -r {{recal}}'.format(list2input(input_bams))
    
    
class UG(Task):
    __verbose__ = "Unified Genotyper"
    inputs = ['bam']
    outputs = ['vcf']
    
    @fromtags('interval','glm')
    def cmd(self,input_bams,glm,interval):
        return 'UnifiedGenotyper -I {0} -glm {{glm}} -L {{interval}}'.format(list2input(input_bams))
    
class CV(Task):
    __verbose__ = "Combine Variants"
    
    inputs = ['vcf']
    outputs = ['vcf']
    
    def cmd(self,input_vcfs):
        return 'CombineVariants {ins}'.format(ins=list2input(input_vcfs))
    
class VQSR(Task):
    __verbose__ = "Variant Quality Score Recalibration"
    inputs = ['vcf']
    outputs = ['recal']
    
    @opoi
    def cmd(self,input_vcf):
        return 'vqsr {input_vcf} > $OUT.recal'
    
class Apply_VQSR(Task):
    __verbose__ = "Apply VQSR"
    
    inputs = ['vcf','recal']
    outputs = ['vcf']
    
    @opoi
    def cmd(self,input_vcf,recal):
        return 'apply vqsr {input_vcf} {recal} > $OUT.vcf'
    
class ANNOVAR(Task):
    __verbose__ = "Annovar"
    inputs = ['vcf']
    outputs = ['tsv']
    
    @fromtags('database')
    @opoi
    def cmd(self,input_vcf,database):
        return 'annovar {input_vcf} {database}'
    
class PROCESS_ANNOVAR(Task):
    __verbose__ = "Process Annovar"
    inputs = ['tsv']
    outputs = ['tsv']
    
    @opoi
    def cmd(self,input_tsv):
        return 'genomekey {input_tsv}'
    
class MERGE_ANNOTATIONS(Task):
    __verbose__ = "Merge Annotations"
    inputs = ['tsv']
    outputs = ['tsv']
    
    def cmd(self,input_tsvs):
        return 'genomekey merge {0}'.format(','.join(input_tsvs))
    
class SQL_DUMP(Task):
    __verbose__ = "SQL Dump"
    inputs = ['tsv']
    inputs = ['sql']
    
    @opoi
    def cmd(self,input_tsv):
        return 'sql dump {input_tsv}'
    
class ANALYSIS(Task):
    __verbose__ = "Filtration And Analysis"
    inputs = ['sql']
    outputs = ['analysis']
    
    @opoi
    def cmd(self,input_sql):
        return 'analyze {input_sql}'
    