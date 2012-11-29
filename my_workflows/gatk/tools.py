from cosmos.contrib.ezflow.tool import Tool

def list2input(l):
    return " -I ".join(map(lambda x: str(x),l))

class GATK(Tool):
    
    @property
    def bin(self):
        return '{0[GATK_path]}'.format(self.settings)
    
class Picard(Tool):
    
    @property
    def bin(self):
        return 'picard_cmd'

class ALN(Tool):
    __verbose__ = "Reference Alignment"
    forward_input=True
    
    inputs = ['fastq']
    outputs = ['sai']
    
    def cmd(self,i,t,s,p):
        return '{s[bwa_path]} aln -t {self.cpu_req} {s[bwa_reference_fasta_path]} {i[fastq][0]} > $OUT.sai'
    
class SAMPE(Tool):
    __verbose__ = "Paired End Mapping"
    
    inputs = ['fastq','sai']
    outputs = ['sam']
    
    
    def cmd(self,i,t,s,p):
        return r"""
            {s[bwa_path]} sampe
            -f $OUT.sam
            -r "@RG\tID:{t2[RG_ID]}\tLB:{t2[RG_LIB]}\tSM:{t2[sample]}\tPL:{t2[RG_PLATFORM]}"
            {i[fastq][0]} {i[fastq][1]} {i[sai][0]} {i[sai][1]}
            """, {
            't2' : self.parents[0].tags
        }

class MERGE_SAMS(Picard):
    __verbose__ = "Merge Sam Files"
    inputs = ['sam']
    outputs = ['bam']
    
    def cmd(self,i,t,s,p):
        return r"""
            {self.bin} -jar MergeSamFiles.jar
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
    
    inputs = ['bam']
    outputs = ['bam']
    
    def cmd(self,i,t,s,p):
        return r"""
            {self.bin} -jar CleanSam.jar
            I={i[bam][0]}
            O=$OUT.bam'
        """

class DEDUPE(Picard):
    __verbose__ = "Mark Duplicates"
    
    inputs = ['bam']
    outputs = ['bam','metrics']
    
    def cmd(self,i,t,s,p):
        return r"""
            {self.bin} -jar MarkDuplicates.jar
            I={i[bam]}
            O=$OUT.bam
            METRICS_FILE=$OUT.metrics
            ASSUME_SORTED=True
        """

class INDEX_BAM(Picard):
    __verbose__ = "Index Bam Files"
    forward_input = True
    
    inputs = ['bam']
    
    def cmd(self,i,t,s,p):
        return r"""
            {self.bin} -jar BuildBamIndex.jar
            INPUT={i[bam]}
            OUTPUT={i[bam]}.bai
        """
                
class RTC(GATK):
    __verbose__ = "Indel Realigner Target Creator"
    forward_input = True
    
    inputs = ['bam']
    outputs = ['intervals']
    
    
    def cmd(self,i,t,s,p):
        return r"""
            {self.bin}
            -T RealignerTargetCreator
            -R {s[reference_fasta_path]}
            -I {i[bam]}
            -o $OUT.intervals
            --known {s[indels_1000g_phase1_path]}
            --known {s[mills_path]}
            -nt {self.cpu_req}
            -L {t[interval]}
        """
    
class IR(GATK):
    __verbose__ = "Indel Realigner"
    inputs = ['bam','intervals']
    outputs = ['bam']
    
    def cmd(self,i,t,s,p):
        return r"""
            {self.bin}
            -T IndelRealigner
            -R {s[reference_fasta_path]}
            -I {i[bam][0]}
            -o $OUT.bam
            -targetIntervals {i[intervals][0]}
            -known {s[indels_1000g_phase1_path]}
            -known {s[mills_path]}
            -model USE_READS
            -L {t[interval]}
        """
    
class BQSR(GATK):
    __verbose__ = "Base Quality Score Recalibration"
    inputs = ['bam']
    outputs = ['recal']
    
    def cmd(self,i,t,s,p):
        return r"""
            {self.bin}
            -T BaseRecalibrator
            -R {s[reference_fasta_path]}
            {inputs}
            -o $OUT.recal
            -knownSites {s[indels_1000g_phase1_path]}
            -knownSites {s[mills_path]}
            --disable_indel_quals
            -cov ReadGroupCovariate
            -cov QualityScoreCovariate
            -cov CycleCovariate
            -cov ContextCovariate
            -nt {self.cpu_req}
        """, {
            'inputs' : list2input(i['bam'])
          }
    
class PR(GATK):
    __verbose__ = "Apply BQSR"
    inputs = ['bam','recal']
    outputs = ['bam']
    
    def map_inputs(self):
        input_bams = [p.get_output('bam') for p in self.parent.parents ] 
        return {'bam' : input_bams,
               'recal' : self.parent.get_output('recal')
              }
    
    def cmd(self,i,t,s,p):
        return 'PrintReads -I {0} -r {{i[recal]}}'.format(list2input(i['bam']))
    
    
class UG(GATK):
    __verbose__ = "Unified Genotyper"
    inputs = ['bam']
    outputs = ['vcf']
    
    def cmd(self,i,t,s,p):
        return 'UnifiedGenotyper -I {0} -glm {{t[glm]}} -L {{t[interval]}}'.format(list2input(i['bam']))
    
class CV(GATK):
    __verbose__ = "Combine Variants"
    
    inputs = ['vcf']
    outputs = ['vcf']
    
    def cmd(self,i,t,s,p):
        return 'CombineVariants {0}'.format(list2input(i['vcf']))
    
class VQSR(GATK):
    __verbose__ = "Variant Quality Score Recalibration"
    inputs = ['vcf']
    outputs = ['recal']
    
#    @opoi
    def cmd(self,i,t,s,p):
        return 'vqsr {i[vcf][0]} > $OUT.recal'
    
class Apply_VQSR(GATK):
    __verbose__ = "Apply VQSR"
    
    inputs = ['vcf','recal']
    outputs = ['vcf']
    
    def map_inputs(self):
        return {'recal': self.parent.get_output('recal'),
                 'vcf': self.parent.parent.get_output('vcf')
                }
    
    def cmd(self,i,t,s,p):
        return 'apply vqsr {i[vcf]} {i[recal]} > $OUT.vcf'
    
class ANNOVAR(Tool):
    __verbose__ = "Annovar"
    inputs = ['vcf']
    outputs = ['tsv']
    
    def cmd(self,i,t,s,p):
        return 'annovar {i[vcf][0]} {t[database]}'
    
class PROCESS_ANNOVAR(Tool):
    __verbose__ = "Process Annovar"
    inputs = ['tsv']
    outputs = ['tsv']
    
#    @opoi
    def cmd(self,i,t,s,p):
        return 'genomekey {i[tsv][0]}'
    
class MERGE_ANNOTATIONS(Tool):
    __verbose__ = "Merge Annotations"
    inputs = ['tsv']
    outputs = ['tsv']
    
    def cmd(self,i,t,s,p):
        return 'genomekey merge {0}'.format(','.join(map(lambda x:str(x),i['tsv'])))
    
class SQL_DUMP(Tool):
    __verbose__ = "SQL Dump"
    inputs = ['tsv']
    outputs = ['sql']
    
    def cmd(self,i,t,s,p):
        return 'sql dump {i[tsv][0]}'
    
class ANALYSIS(Tool):
    __verbose__ = "Filtration And Analysis"
    inputs = ['sql']
    outputs = ['analysis']
    
    def cmd(self,i,t,s,p):
        return 'analyze {i[sql][0]}'
        
    