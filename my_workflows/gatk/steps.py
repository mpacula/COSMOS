from django.core.exceptions import ValidationError
from cosmos.contrib import step
import settings
from settings import get_Picard_cmd, get_GATK_cmd

step.default_pcmd_dict = {'settings':settings }

class BWA_Align(step.Step):
    outputs = {'sai':'aln.sai'}
    mem_req = 3.5*1024
    cpu_req = 2 #4
    
    def none2many_cmd(self,data_dict):
        """
        :para: input_batch: a place holder and is ignored
        :param data_dict: a list of fastq file dictionaries
        """
        for f in data_dict:
            yield {
                    'pcmd':r"""
                               {settings.bwa_path} aln -t {self.cpu_req} {settings.bwa_reference_fasta_path} {fastq_path} > {{output_dir}}/{{outputs[sai]}}
                            """,
                    'pcmd_dict': {'fastq_path':f['path'] },
                    'add_tags': {'sample':f['sample'],
                                'lane': f['lane'],
                                'fq_chunk': f['chunk'],
                                'fq_pair': f['pair'],
                                'fq_path': f['path'],
                                'RG_ID':'%s.L%s' % (f['flowcell'],f['lane']),
                                'RG_LIB': f['library'],
                                'RG_PLATFORM': f['platform']
                                },
                    'name' : step.dict2node_name({'sample':f['sample'],'lane':f['lane'],'fq_chunk':f['chunk'],'fq_pair':f['pair']})
                }


class BWA_Sampe(step.Step):
    outputs = {'sam':'raw.sam'}
    mem_req = 5*1024
    cpu_req = 1 #4
    
    def many2one_cmd(self,input_nodes,tags):
        node_r1 = input_nodes[0]
        node_r2 = input_nodes[1]
        return {
            'pcmd': r"""
                    {settings.bwa_path} sampe
                    -f  {{output_dir}}/{{outputs[sam]}}
                    -r "@RG\tID:{node_r1.tags[RG_ID]}\tLB:{node_r1.tags[RG_LIB]}\tSM:{node_r1.tags[sample]}\tPL:{node_r1.tags[RG_PLATFORM]}"
                    {settings.bwa_reference_fasta_path}
                    {node_r1.output_paths[sai]}
                    {node_r2.output_paths[sai]}
                    {node_r1.tags[fq_path]}
                    {node_r2.tags[fq_path]}
                """,
            'pcmd_dict':{'node_r1': node_r1,
                         'node_r2': node_r2 }
             
        }

class CleanSam(step.Step):
    outputs = {'bam':'cleaned.bam'}
    mem_req = 2*1024
    
    def one2one_cmd(self,input_node,input_type='bam'):
        return {
            'pcmd': r"""
                        {Picard_cmd}
                        I= {input}
                        O={{output_dir}}/{{outputs[bam]}}
                    """,
            'pcmd_dict': {'Picard_cmd':get_Picard_cmd('CleanSam.jar',self.mem_req),
                          'input':input_node.output_paths[input_type]}
        }

class MergeSamFiles(step.Step):
    outputs = {'bam':'merged.bam'}
    mem_req = 3*1024
    
    def many2one_cmd(self,input_nodes,tags,assume_sorted=True):
        INPUTs = "\n".join(["INPUT={0}".format(n.output_paths['bam']) for n in input_nodes])
        return {
            'pcmd': r"""
                        {Picard_cmd}
                        {INPUTs}
                        OUTPUT={{output_dir}}/{{outputs[bam]}}
                        SORT_ORDER=coordinate
                        MERGE_SEQUENCE_DICTIONARIES=True
                        ASSUME_SORTED={assume_sorted}
                    """,
            'pcmd_dict': {'Picard_cmd':get_Picard_cmd('MergeSamFiles.jar',self.mem_req),
                         'INPUTs':INPUTs,
                         'assume_sorted':assume_sorted
                         }
        }

class MarkDuplicates(step.Step):
    outputs = {'bam':'deduped.bam',
               'metrics_file':'metrics.log'}
    mem_req = 5*1024
    cpu_req = 1
    
    def one2one_cmd(self,input_node,assume_sorted=True):
        return {
            'pcmd': r"""
                        {Picard_cmd}
                        I={input_node.output_paths[bam]}
                        O={{output_dir}}/{{outputs[bam]}}
                        METRICS_FILE={{output_dir}}/{{outputs[metrics_file]}}
                        ASSUME_SORTED={assume_sorted}
                    """,
            'pcmd_dict' : {'Picard_cmd':get_Picard_cmd('MarkDuplicates.jar',self.mem_req),
                         'assume_sorted':assume_sorted}
        }
    
class BuildBamIndex(step.Step):
    mem_req = 2*1024
    
    def one2one_cmd(self,input_node):
        return {
            'pcmd': r"""
                {Picard_cmd}
                INPUT={input_node.output_paths[bam]}
                OUTPUT={input_node.output_paths[bam]}.bai
            """,
            'pcmd_dict': {'Picard_cmd':get_Picard_cmd('BuildBamIndex.jar',self.mem_req)}
        }

class RealignerTargetCreator(step.Step):
    outputs = {'targetIntervals':'target.intervals'}
    mem_req = 2.5*1024
    cpu_req = 1

    def multi_one2many_cmd(self,input_node_dict,intervals):
        for interval in intervals:
            yield {
                'pcmd': r"""
                    {GATK_cmd}
                    -T RealignerTargetCreator
                    -R {settings.reference_fasta_path}
                    -I {input_bam}
                    -o {{output_dir}}/{{outputs[targetIntervals]}}
                    --known {settings.indels_1000g_phase1_path}
                    --known {settings.mills_path}
                    -nt {self.cpu_req}
                    -L {interval}
                """,
                'pcmd_dict':{'GATK_cmd':get_GATK_cmd(mem_req=self.mem_req),
                             'input_bam': input_node_dict['MarkDuplicates'].output_paths['bam'],
                             'interval':interval},
                'add_tags':{'interval':interval}
            }

class IndelRealigner(step.Step):
    outputs = {'bam':'realigned.bam'}
    mem_req = 2.5*1024
    cpu_req = 1 #-nt is not supported by this tool
    
    def multi_one2one_cmd(self,input_node_dict,model='USE_READS'):
        
        """
        Expects rtc_batch to have been split by the same intervals.
        
        :param rtc_batch: the realigner target creator batch.  Its expected that it was parallelized by the same intervals as :param:`intervals`
        :param intervals: the intervals to split by.  Usually a list of chromosomes.
        :param model: USE_READS or KNOWNS_ONLY
        """
        if model == 'KNOWNS_ONLY':
            raise NotImplementedError()
            
        return {
            'pcmd': r"""
                        {GATK_cmd}
                        -T IndelRealigner
                        -R {settings.reference_fasta_path}
                        -I {input_bam}
                        -o {{output_dir}}/{{outputs[bam]}}
                        -targetIntervals {targetIntervals}
                        -known {settings.indels_1000g_phase1_path}
                        -known {settings.mills_path}
                        -model {model}
                        -L {interval}
                    """,
            'pcmd_dict':{'GATK_cmd':get_GATK_cmd(mem_req=self.mem_req),
                         'model':model,
                         'input_bam': input_node_dict['MarkDuplicates'].output_paths['bam'],
                         'targetIntervals':input_node_dict['RealignerTargetCreator'].output_paths['targetIntervals'],
                         'interval':input_node_dict['RealignerTargetCreator'].tags['interval']}
        }

class BaseQualityScoreRecalibration(step.Step):
    outputs = {'recal':'bqsr.recal'}
    mem_req = 2.5*1024
    cpu_req = 1 #>1 results in ##### ERROR MESSAGE: We have temporarily disabled the ability to run BaseRecalibrator multi-threaded for performance reasons.  We hope to have this fixed for the next GATK release (2.2) and apologize for the inconvenience.
    
    pcmd = r"""
        {GATK_cmd}
        -T BaseRecalibrator
        -R {settings.reference_fasta_path}
        {INPUTs}
        -o {{output_dir}}/{{outputs[recal]}}
        -knownSites {settings.indels_1000g_phase1_path}
        -knownSites {settings.mills_path}
        --disable_indel_quals
        -cov ReadGroupCovariate
        -cov QualityScoreCovariate
        -cov CycleCovariate
        -cov ContextCovariate
        -nt {self.cpu_req}
    """
    
    def many2one_cmd(self,input_nodes,tags):
        INPUTs = ' '.join([ '-I {0}'.format(n.output_paths['bam']) for n in input_nodes ])
        return {
            'pcmd': self.pcmd,
            'pcmd_dict': {'GATK_cmd':get_GATK_cmd(mem_req=self.mem_req),
                          'INPUTs':INPUTs}
        }

class PrintReads(step.Step):
    outputs = {'bam':'recalibrated.bam'}
    mem_req = 5*1024
    cpu_req = 1
    
    pcmd = r"""
        {GATK_cmd}
        -T PrintReads
        -R {settings.reference_fasta_path}
        {INPUTs}
        -o {{output_dir}}/{{outputs[bam]}} 
        -BQSR {recal_file}
    """
    
    def multi_many2one_cmd(self,input_nodes_dict):
        INPUTs = ' '.join([ '-I {0}'.format(n.output_paths['bam']) for n in input_nodes_dict['IndelRealigner']])
        return {
            'pcmd': self.pcmd,
            'pcmd_dict': {'GATK_cmd':get_GATK_cmd(mem_req=self.mem_req),
                          'INPUTs':INPUTs,
                          'recal_file': input_nodes_dict['BaseQualityScoreRecalibration'][0].output_paths['recal']}
        }
    
    
class UnifiedGenotyper(step.Step):
    outputs = {'vcf':'raw.vcf'}
    mem_req = 5.5*1024
    cpu_req = 6
    
    def many2many_cmd(self,input_nodes,tags,intervals):
        """
        UnifiedGenotyper takes as input all bams [sample1.bam,sample2.bam...sample3.bam]
        
        :param input_batch: sample level bams
        """
        input_bams = ' '.join([ '-I {0}'.format(n.output_paths['bam']) for n in input_nodes ])
        for glm in ['SNP','INDEL']:
            for interval in intervals:
                yield {
                    'pcmd':r"""
                                {GATK_cmd}
                                -T UnifiedGenotyper
                                -R {settings.reference_fasta_path}
                                --dbsnp {settings.dbsnp_path}
                                -glm {glm}
                                {input_bams}
                                -o {{output_dir}}/{{outputs[vcf]}}
                                -A DepthOfCoverage
                                -A HaplotypeScore
                                -A InbreedingCoeff
                                -baq CALCULATE_AS_NECESSARY
                                -L {interval}
                                -nt {self.cpu_req}
                            """,
                    'pcmd_dict': {'GATK_cmd':get_GATK_cmd(mem_req=self.mem_req),
                                  'input_bams':input_bams,
                                  'interval':interval,
                                  'glm':glm},
                    'add_tags':{'interval':interval,
                                'glm':glm}
                }


class CombineVariants(step.Step):
    outputs = {'vcf':'combined.vcf'}
    mem_req = 2*1024

    def many2one_cmd(self,input_nodes,tags,genotypeMergeOptions='UNSORTED'):
        """
        :param genotypemergeoptions: select from the following
            UNIQUIFY - Make all sample genotypes unique by file. Each sample shared across RODs gets named sample.ROD.
            PRIORITIZE - Take genotypes in priority order (see the priority argument).
            UNSORTED - Take the genotypes in any order.
            REQUIRE_UNIQUE - Require that all samples/genotypes be unique between all inputs.
        """
        INPUTs = "\n".join(["--variant {0}".format(n.output_paths['vcf']) for n in input_nodes])
        return {
                'pcmd': r"""
                        {GATK_cmd}
                        -T CombineVariants
                        -R {settings.reference_fasta_path}
                        {INPUTs}
                        -o {{output_dir}}/{{outputs[vcf]}}
                        -genotypeMergeOptions {genotypeMergeOptions}
                    """,
                'pcmd_dict': {'GATK_cmd':get_GATK_cmd(mem_req=self.mem_req),
                              'INPUTs':INPUTs,
                              'genotypeMergeOptions':genotypeMergeOptions}
            }
        
class VariantQualityRecalibration(step.Step):
    outputs = {'recal':'vqr.recal',
               'tranches':'vqr.tranches',
               'rscript':'plot.R'
               }
    mem_req = 3.5*1024
    
    def one2one_cmd(self,input_node,exome_or_wgs,haplotypeCaller_or_unifiedGenotyper='UnifiedGenotyper',inbreeding_coeff=True):
        """
        :param exome_or_wgs: choose from ('exome','wgs')
        :param haplotypeCaller_or_unifiedGenotyper: choose from ('Haplotypecaller','UnifiedGenotyper')
        :param inbreedingcoeff: True to use the inbreedingcoeff annotation, False to not use it
        
        ..note:: inbreedingcoeff can only be calculated if there are more than 20 samples
        """
        #validation
        glm = input_node.tags['glm']
        if glm not in ['SNP','INDEL']:
            raise ValidationError('invalid parameter')
        if exome_or_wgs not in ['exome','wgs']:
            raise ValidationError('invalid parameter')
        if haplotypeCaller_or_unifiedGenotyper not in ['HaplotypeCaller','UnifiedGenotyper']:
            raise ValidationError('invalid parameter')
        
        InbreedingCoeff = ''
        if inbreeding_coeff:
            InbreedingCoeff = '-an InbreedingCoeff'
        
        if exome_or_wgs == 'exome':
            if haplotypeCaller_or_unifiedGenotyper == 'UnifiedGenotyper':
                if glm == 'SNP': 
                    cmd = r"""
                    {GATK_cmd}
                    -T VariantRecalibrator
                    -R {settings.reference_fasta_path}
                    -input {input_vcf}
                    --maxGaussians 6
                    -resource:hapmap,known=false,training=true,truth=true,prior=15.0 {settings.hapmap_path}
                    -resource:omni,known=false,training=true,truth=false,prior=12.0 {settings.omni_path}
                    -resource:dbsnp,known=true,training=false,truth=false,prior=6.0 {settings.dbsnp_path}
                    -an QD -an HaplotypeScore -an MQRankSum -an ReadPosRankSum -an FS -an MQ {InbreedingCoeff}
                    -mode SNP
                    -recalFile {{output_dir}}/{{outputs[recal]}}
                    -tranchesFile {{output_dir}}/{{outputs[tranches]}}
                    -rscriptFile {{output_dir}}/{{outputs[rscript]}}
                    """
                elif glm == 'INDEL':
                    cmd = r"""
                    {GATK_cmd}
                    -T VariantRecalibrator
                    -R {settings.reference_fasta_path}
                    -input {input_vcf}
                    --maxGaussians 4 -std 10.0 -percentBad 0.12
                    -resource:mills,known=true,training=true,truth=true,prior=12.0 {settings.mills_path}
                    -an QD -an FS -an HaplotypeScore -an ReadPosRankSum -an {InbreedingCoeff}
                    -mode INDEL
                    -recalFile {{output_dir}}/{{outputs[recal]}}
                    -tranchesFile {{output_dir}}/{{outputs[tranches]}}
                    -rscriptFile {{output_dir}}/{{outputs[rscript]}}
                    """
            elif haplotypeCaller_or_unifiedGenotyper == 'HaplotypeCaller':
                raise NotImplementedError()
                if glm == 'SNP' or glm == 'INDEL': 
                    cmd = r"""
                    {GATK_cmd}
                    -T VariantRecalibrator
                    -R {settings.reference_fasta_path}
                    -input {input_vcf}
                    --maxGaussians 6
                    -resource:hapmap,known=false,training=true,truth=true,prior=15.0 hapmap_3.3.b37.sites.vcf
                    -resource:omni,known=false,training=true,truth=false,prior=12.0 1000G_omni2.5.b37.sites.vcf
                    -resource:mills,known=true,training=true,truth=true,prior=12.0 Mills_and_1000G_gold_standard.indels.b37.sites.vcf
                    -resource:dbsnp,known=true,training=false,truth=false,prior=6.0 dbsnp_135.b37.vcf
                    -an QD -an MQRankSum -an ReadPosRankSum -an FS -an MQ -an InbreedingCoeff -an ClippingRankSum
                    -mode BOTH
                    -recalFile {{output_dir}}/{{outputs[recal]}}
                    -tranchesFile {{output_dir}}/{{outputs[tranches]}}
                    -rscriptFile {{output_dir}}/{{outputs[rscript]}}
                    """
        elif glm == 'wgs':
            raise NotImplementedError()
        
        return {
            'pcmd' : cmd,
            'pcmd_dict': {'GATK_cmd':get_GATK_cmd(mem_req=self.mem_req),
                          'input_vcf':input_node.output_paths['vcf'],
                          'InbreedingCoeff':InbreedingCoeff}
        } 

class ApplyRecalibration(step.Step):
    outputs = {'vcf':'recalibrated.vcf'}
    mem_req = 2*1024
    
    def multi_one2one_cmd(self,input_node_dict,haplotypeCaller_or_unifiedGenotyper='UnifiedGenotyper'):
        """
        :param haplotypeCaller_or_unifiedGenotyper: choose from ('Haplotypecaller','UnifiedGenotyper')
        
        ..note:: inbreedingcoeff is only calculated if there are more than 20 samples
        """
        #validation
        raw_variants_node = input_node_dict['CombineVariants']
        vqr_node = input_node_dict['VariantQualityRecalibration']
        glm = raw_variants_node.tags['glm']
        
        
        if haplotypeCaller_or_unifiedGenotyper == 'UnifiedGenotyper':
            if glm == 'SNP': 
                cmd = r"""
                {GATK_cmd}
                -T ApplyRecalibration
                -R {settings.reference_fasta_path}
                -input {input_vcf}
                -tranchesFile {input_tranches}
                -recalFile {input_recal}
                -o {{output_dir}}/{{outputs[vcf]}}
                --ts_filter_level 99.0
                -mode SNP
                """
            elif glm == 'INDEL':
                cmd = r"""
                {GATK_cmd}
                -T ApplyRecalibration
                -R {settings.reference_fasta_path}
                -input {input_vcf}
                -tranchesFile {input_tranches}
                -recalFile {input_recal}
                -o {{output_dir}}/{{outputs[vcf]}}
                --ts_filter_level 95.0
                -mode INDEL
                """
        elif haplotypeCaller_or_unifiedGenotyper == 'HaplotypeCaller':
            raise NotImplementedError()
            if glm == 'SNP' or glm == 'INDEL': 
                cmd = r"""
                {GATK_cmd}
                -T ApplyRecalibration
                -R {settings.reference_fasta_path}
                -input {input_vcf}
                -tranchesFile {input_tranches}
                -recalFile {input_recal}
                -o {{output_dir}}/{{outputs[vcf]}}
                --ts_filter_level 97.0
                -mode BOTH
                """
        return {
            'pcmd': cmd,
            'pcmd_dict': {'GATK_cmd':get_GATK_cmd(mem_req=self.mem_req),
                          'input_vcf':raw_variants_node.output_paths['vcf'],
                         'input_tranches':vqr_node.output_paths['tranches'],
                         'input_recal':vqr_node.output_paths['recal'],}
        }

