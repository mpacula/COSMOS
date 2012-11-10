from task import Task
from decorators import pformat

class FASTQ(Task):
    __verbose_name__ = "Fastq Input"
    
    outputs = ['fastq']
    
    def map_cmd(self):
        return ''

class ALN(Task):
    __verbose_name__ = "Reference Alignment"
    
    outputs = ['fastq','sai']
    
    def map_cmd(self):
        fastq = self.parent.output_paths['fastq']
        return self.cmd(fastq)
    
    def cmd(self,fastq):
        return 'bwa aln {fastq} > $OUT.sai'.format(fastq=fastq)
    
class SAMPE(Task):
    __verbose_name__ = "Paired End Mapping"
    
    outputs = ['sam']
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
    
    def map_cmd(self):
        sams = [ p.output_paths['bam'] for p in self.parents ]
        return self.cmd(sams)
    
    @pformat
    def cmd(self,sams):
        return 'cleansam {sams} -o $OUT.bam'
    
class IRTC(Task):
    __verbose_name__ = "Indel Realigner Target Creator"
    
    inputs = ['bam']
    outputs = ['targets']
    
    def map_cmd(self):
        bams = [ p.output_paths['bam'] for p in self.parents ]
        return self.cmd(bams,self.tags['interval'])
    
    @pformat
    def cmd(self,input_bams,interval):
        return 'IRTC -I {{input_bams}} -L {{interval}} > $OUT.targets'.format(' -I '.join(input_bams))
    
class IR(Task):
    __verbose_name__ = "Indel Realigner"
    inputs = ['bam','targets']
    outputs = ['bam']
    
    def map_cmd(self):
        input_bam = self.parent.parent.output_paths['bam']
        return self.cmd(input_bam,self.tags['interval'],self.parent.output_paths['targets'])
    
    @pformat
    def cmd(self,input_bam,interval,targets):
        return 'IR -I {input_bam} -L {interval} -t {targets}'
    
class BQSR(Task):
    __verbose_name__ = "Base Quality Score Recalibration"
    inputs = ['bam']
    outputs = ['recal']
    
    def map_cmd(self):
        bams = [ p.output_paths['bam'] for p in self.parents ]
        return self.cmd(bams,self.tags['interval'])
    
    @pformat
    def cmd(self,input_bams,interval):
        return 'BQSR -I {0} -L {{interval}} > $OUT.recal'.format(' -I '.join(input_bams))
    
class PR(Task):
    __verbose_name__ = "Apply BQSR"
    inputs = ['bam','recal']
    outputs = ['bam']
    
    def map_cmd(self):
        bams = [ p.output_paths['bam'] for p in self.parent.parents ]
        return self.cmd(bams,self.tags['interval'])
    
    @pformat
    def cmd(self,input_bam,interval):
        return 'IR -I {input_bam} -L {interval}'
    
    
class UG(Task):
    __verbose_name__ = "Unified Genotyper"
    inputs = ['bam']
    outputs = ['vcf']
    def map_cmd(self):
        bams = [ p.output_paths['bam'] for p in self.parents ]
        return self.cmd(bams,self.tags['glm'],self.tags['interval'])
    
    @pformat
    def cmd(self,input_bams,glm,interval):
        return 'UnifiedGenotyper -I {0} -glm {{glm}} -L {{interval}}'.format(' -I '.join(input_bams))
    
class CV(Task):
    __verbose_name__ = "Combine Variants"
    
    inputs = ['vcf']
    outputs = ['vcf']
    
    @pformat
    def cmd(self,input_vcfs):
        ins = '-I '.join(input_vcfs)
        return 'CombineVariants {ins}'.format(ins=ins)
    
class VQSR(Task):
    __verbose_name__ = "Variant Quality Score Recalibration"
    inputs = ['vcf']
    outputs = ['recal']
    
    @pformat
    def cmd(self,input_vcf):
        return 'vqsr {input_vcf} > $OUT.recal'
    
class Apply_VQSR(Task):
    __verbose_name__ = "Apply VQSR"
    
    inputs = ['vcf','recal']
    outputs = ['vcf']
    
    @pformat
    def cmd(self,input_vcf,recal):
        return 'apply vqsr {input_vcf} {recal} > $OUT.vcf'
    
class ANNOVAR(Task):
    __verbose_name__ = "Annovar"
    inputs = ['vcf']
    outputs = ['tsv']
    
    @pformat
    def cmd(self,input_vcf):
        return ''
    
class PROCESS_ANNOVAR(Task):
    __verbose_name__ = "Process Annovar"
    inputs = ['tsv']
    outputs = ['tsv']
    
    def cmd(self,input_tsv):
        return ''
    
class MERGE_ANNOTATIONS(Task):
    __verbose_name__ = "Merge Annotations"
    inputs = ['tsv']
    outputs = ['tsv']
    
    def cmd(self,input_tsv):
        return ''
    
class SQL_DUMP(Task):
    __verbose_name__ = "SQL Dump"
    inputs = ['tsv']
    inputs = ['sql']
    
    def cmd(self,input_tsv):
        return ''
    
class ANALYSIS(Task):
    __verbose_name__ = "Filtration And Analysis"
    inputs = ['sql']
    outputs = ['analysis']
    
    def cmd(self,input_sql):
        return ''
    