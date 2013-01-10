from cosmos.contrib.ezflow.tool import Tool
import os

def list2input(l):
    return "-I " +" -I ".join(map(lambda x: str(x),l))

class GATK(Tool):
    time_req = 5*60
    mem_req = 5*1024

    @property
    def bin(self):
        return 'java -Xmx{mem_req}m -Djava.io.tmpdir={s[tmp_dir]} -jar {s[GATK_path]}'.format(self=self,s=self.settings,mem_req=int(self.mem_req*.8))

class RTC(GATK):
    __verbose__ = "Indel Realigner Target Creator"
    mem_req = 4.5*1024
    cpu_req = 4
    inputs = ['bam']
    outputs = ['intervals']
    forward_input = True
    one_parent = True
    
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
    mem_req = 4.5*1024
    inputs = ['bam','intervals']
    outputs = ['bam']
    one_parent = True
    
    def cmd(self,i,t,s,p):
        return r"""
            {self.bin}
            -T IndelRealigner
            -R {s[reference_fasta_path]}
            -I {i[bam]}
            -o $OUT.bam
            -targetIntervals {i[intervals]}
            -known {s[indels_1000g_phase1_path]}
            -known {s[mills_path]}
            -model USE_READS
            -L {t[interval]}
        """
    
class BQSR(GATK):
    __verbose__ = "Base Quality Score Recalibration"
    cpu_req = 1
    mem_req = 9*1024
    inputs = ['bam']
    outputs = ['recal']

    # -nct {nct}

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
        """, {
            'inputs' : list2input(i['bam']),
            'nct': self.cpu_req +1
          }
    
class PR(GATK):
    __verbose__ = "Apply BQSR"
    mem_req = 8*1024
    inputs = ['bam','recal']
    outputs = ['bam']
    
    def map_inputs(self):
        input_bams = [p.get_output('bam') for p in self.parent.parents ]
        return {'bam' : input_bams,
               'recal' : self.parent.get_output('recal')
              }
    
    def cmd(self,i,t,s,p):
        return r"""
            {self.bin}
            -T PrintReads
            -R {s[reference_fasta_path]}
            {inputs}
            -o $OUT.bam
            -BQSR {i[recal]}
        """, {
            'inputs' : list2input(i['bam'])  
        }

    
class UG(GATK):
    __verbose__ = "Unified Genotyper"
    mem_req = 5.5*1024
    inputs = ['bam']
    outputs = ['vcf']
    
    def cmd(self,i,t,s,p):
        return r"""
            {self.bin}
            -T UnifiedGenotyper
            -R {s[reference_fasta_path]}
            --dbsnp {s[dbsnp_path]}
            -glm {t[glm]}
            {inputs}
            -o $OUT.vcf
            -A DepthOfCoverage
            -A HaplotypeScore
            -A InbreedingCoeff
            -baq CALCULATE_AS_NECESSARY
            -L {t[interval]}
            -nt {self.cpu_req}
        """, {
            'inputs' : list2input(i['bam']) 
        }
    
class CV(GATK):
    __verbose__ = "Combine Variants"
    mem_req = 4*1024
    
    inputs = ['vcf']
    outputs = ['vcf']
    
    default_params = {
      'genotypeMergeOptions':'UNSORTED'       
    }
    
    def cmd(self,i,t,s,p):
        """
        :param genotypemergeoptions: select from the following:
            UNIQUIFY - Make all sample genotypes unique by file. Each sample shared across RODs gets named sample.ROD.
            PRIORITIZE - Take genotypes in priority order (see the priority argument).
            UNSORTED - Take the genotypes in any order.
            REQUIRE_UNIQUE - Require that all samples/genotypes be unique between all inputs.
        """
        return r"""
            {self.bin}
            -T CombineVariants
            -R {s[reference_fasta_path]}
            {inputs}
            -o $OUT.vcf
            -genotypeMergeOptions {p[genotypeMergeOptions]}
        """, {
            'inputs' : "\n".join(["--variant {0}".format(vcf) for vcf in i['vcf']])
        }
    
class VQSR(GATK):
    __verbose__ = "Variant Quality Score Recalibration"
    mem_req = 4*1024
    inputs = ['vcf']
    outputs = ['recal','tranches','R']
    
    forward_input = True
    one_parent = True
    
    default_params = {
      'inbreeding_coeff' : False
    }
    
    def cmd(self,i,t,s,p):
        if t['glm'] == 'SNP': 
            cmd = r"""
            {self.bin}
            -T VariantRecalibrator
            -R {s[reference_fasta_path]}
            -input {i[vcf]}
            --maxGaussians 6
            -resource:hapmap,known=false,training=true,truth=true,prior=15.0 {s[hapmap_path]}
            -resource:omni,known=false,training=true,truth=false,prior=12.0 {s[omni_path]}
            -resource:dbsnp,known=true,training=false,truth=false,prior=6.0 {s[dbsnp_path]}
            -an QD -an HaplotypeScore -an MQRankSum -an ReadPosRankSum -an FS -an MQ {InbreedingCoeff}
            -mode SNP
            -recalFile $OUT.recal
            -tranchesFile $OUT.tranches
            -rscriptFile $OUT.R
            """
        elif t['glm'] == 'INDEL':
            cmd = r"""
            {self.bin}
            -T VariantRecalibrator
            -R {s[reference_fasta_path]}
            -input {i[vcf]}
            --maxGaussians 4 -std 10.0 -percentBad 0.12
            -resource:mills,known=true,training=true,truth=true,prior=12.0 {s[mills_path]}
            -an QD -an FS -an HaplotypeScore -an ReadPosRankSum {InbreedingCoeff}
            -mode INDEL
            -recalFile $OUT.recal
            -tranchesFile $OUT.tranches
            -rscriptFile $OUT.R
            """
        return cmd, {'InbreedingCoeff' : '-an InbreedingCoeff' if p['inbreeding_coeff'] else '' }
    
class Apply_VQSR(GATK):
    __verbose__ = "Apply VQSR"
    mem_req = 4*1024
    
    inputs = ['vcf','recal','tranches']
    outputs = ['vcf']
    one_parent = True
    
    def cmd(self,i,t,s,p):
        if t['glm'] == 'SNP': 
            cmd = r"""
            {self.bin}
            -T ApplyRecalibration
            -R {s[reference_fasta_path]}
            -input {i[vcf]}
            -tranchesFile {i[tranches]}
            -recalFile {i[recal]}
            -o $OUT.vcf
            --ts_filter_level 99.0
            -mode SNP
            """
        elif t['glm'] == 'INDEL':
            cmd = r"""
            {self.bin}
            -T ApplyRecalibration
            -R {s[reference_fasta_path]}
            -input {i[vcf]}
            -tranchesFile {i[tranches]}
            -recalFile {i[recal]}
            -o $OUT.vcf
            --ts_filter_level 95.0
            -mode INDEL
            """
        return cmd
    
    
class ANNOVAR(Tool):
    __verbose__ = "Annovar"
    inputs = ['vcf']
    outputs = ['tsv']
    one_parent = True
    
    def cmd(self,i,t,s,p):
        return 'annovar {i[vcf]} {t[database]}'
    
class PROCESS_ANNOVAR(Tool):
    __verbose__ = "Process Annovar"
    inputs = ['tsv']
    outputs = ['tsv']
    one_parent = True

    def cmd(self,i,t,s,p):
        return 'genomekey {i[tsv]}'
    
class MERGE_ANNOTATIONS(Tool):
    __verbose__ = "Merge Annotations"
    inputs = ['tsv']
    outputs = ['tsv']
    one_parent = True
    
    def cmd(self,i,t,s,p):
        return 'genomekey merge {0}'.format(','.join(map(lambda x:str(x),i['tsv'])))
    
class SQL_DUMP(Tool):
    __verbose__ = "SQL Dump"
    inputs = ['tsv']
    outputs = ['sql']
    one_parent = True
    
    def cmd(self,i,t,s,p):
        return 'sql dump {i[tsv]}'
    
class ANALYSIS(Tool):
    __verbose__ = "Filtration And Analysis"
    inputs = ['sql']
    outputs = ['analysis']
    one_parent = True
    
    def cmd(self,i,t,s,p):
        return 'analyze {i[sql]}'
        
    