from task import Task
from decorators import pformat, fromtags, opoi

def list2input(l):
    return " -I ".join(l)

class FASTQ(Task):
    __verbose_name__ = "Fastq Input"
    
    outputs = ['fastq']
    
    def map_cmd(self):
        return None

class ALN(Task):
    __verbose_name__ = "Reference Alignment"
    inputs = ['fastq']
    outputs = ['sai']
    
    @opoi
    @pformat
    def cmd(self,fastq):
        return 'bwa aln {fastq} > $OUT.sai'
    
class SAMPE(Task):
    __verbose_name__ = "Paired End Mapping"
    
    inputs = ['sam']
    outputs = ['bam']
    
    def map_cmd(self):
        "Expecting 2 input nodes"
        ps = self.parents
        return self.cmd(ps[0].output_paths['fastq'],ps[1].output_paths['fastq'],ps[0].output_paths['sai'],ps[1].output_paths['sai'])
    
    @pformat
    def cmd(self,fastq1,fastq2,aln1,aln2):
        return 'bwa sampe {fastq1} {fastq2} {aln1} {aln2} > $OUT.sam'
    
class CLEAN_SAM(Task):
    __verbose_name__ = "Sam Cleaning"
    
    inputs = ['sam']
    outputs=['bam']
    
    @pformat
    def cmd(self,sams):
        return 'cleansam {0} -o $OUT.bam'.format(list2input(sams))
    
class IRTC(Task):
    __verbose_name__ = "Indel Realigner Target Creator"
    
    inputs = ['bam']
    outputs = ['targets']
    
    @fromtags('interval')
    @pformat
    def cmd(self,input_bams,interval):
        return 'IRTC -I {0} -L {{interval}} > $OUT.targets'.format(list2input(input_bams))
    
class IR(Task):
    __verbose_name__ = "Indel Realigner"
    inputs = ['bam','targets']
    outputs = ['bam']
    
    def map_cmd(self):
        input_bam = self.parent.parent.output_paths['bam']
        return self.cmd(input_bam,self.parent.output_paths['targets'],interval=self.tags['interval'])
    
    def cmd(self,input_bam,targets,interval):
        return 'IR -I {input_bam} -L {interval} -t {targets}'
    
class BQSR(Task):
    __verbose_name__ = "Base Quality Score Recalibration"
    inputs = ['bam']
    outputs = ['recal']
    
    @pformat
    def cmd(self,input_bams):
        return 'BQSR -I {0} > $OUT.recal'.format(' -I '.join(input_bams))
    
class PR(Task):
    __verbose_name__ = "Apply BQSR"
    inputs = ['bam','recal']
    outputs = ['bam']
    
    def map_cmd(self):
        input_bams = [p.output_paths['bam'] for p in self.parent.parents ] 
        return self.cmd(input_bams,self.parent.output_paths['recal'])
    
    def cmd(self,input_bams,recal):
        return 'PrintReads -I {0} -r {{recal}}'.format(list2input(input_bams))
    
    
class UG(Task):
    __verbose_name__ = "Unified Genotyper"
    inputs = ['bam']
    outputs = ['vcf']
    
    @fromtags('interval','glm')
    @pformat
    def cmd(self,input_bams,glm,interval):
        return 'UnifiedGenotyper -I {0} -glm {{glm}} -L {{interval}}'.format(list2input(input_bams))
    
class CV(Task):
    __verbose_name__ = "Combine Variants"
    
    inputs = ['vcf']
    outputs = ['vcf']
    
    def cmd(self,input_vcfs):
        return 'CombineVariants {ins}'.format(ins=list2input(input_vcfs))
    
class VQSR(Task):
    __verbose_name__ = "Variant Quality Score Recalibration"
    inputs = ['vcf']
    outputs = ['recal']
    
    @opoi
    @pformat
    def cmd(self,input_vcf):
        return 'vqsr {input_vcf} > $OUT.recal'
    
class Apply_VQSR(Task):
    __verbose_name__ = "Apply VQSR"
    
    inputs = ['vcf','recal']
    outputs = ['vcf']
    
    @opoi
    @pformat
    def cmd(self,input_vcf,recal):
        return 'apply vqsr {input_vcf} {recal} > $OUT.vcf'
    
class ANNOVAR(Task):
    __verbose_name__ = "Annovar"
    inputs = ['vcf']
    outputs = ['tsv']
    
    @fromtags('database')
    @opoi
    def cmd(self,input_vcf,database):
        return 'annovar {input_vcf} {database}'
    
class PROCESS_ANNOVAR(Task):
    __verbose_name__ = "Process Annovar"
    inputs = ['tsv']
    outputs = ['tsv']
    
    @opoi
    @pformat
    def cmd(self,input_tsv):
        return 'genomekey {input_tsv}'
    
class MERGE_ANNOTATIONS(Task):
    __verbose_name__ = "Merge Annotations"
    inputs = ['tsv']
    outputs = ['tsv']
    
    def cmd(self,input_tsvs):
        return 'genomekey merge {0}'.format(','.join(input_tsvs))
    
class SQL_DUMP(Task):
    __verbose_name__ = "SQL Dump"
    inputs = ['tsv']
    inputs = ['sql']
    
    @opoi
    @pformat
    def cmd(self,input_tsv):
        return 'sql dump {input_tsv}'
    
class ANALYSIS(Task):
    __verbose_name__ = "Filtration And Analysis"
    inputs = ['sql']
    outputs = ['analysis']
    
    @opoi
    @pformat
    def cmd(self,input_sql):
        return 'analyze {input_sql}'
    