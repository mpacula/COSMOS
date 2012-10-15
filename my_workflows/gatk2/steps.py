from django.core.exceptions import ValidationError
from Cosmos.addons import step
from Cosmos.helpers import parse_cmd
import settings
from settings import get_Picard_cmd

step.settings = settings

def bwa_aln(fastq,output_sai):
    def _parse_cmd(s,**kwargs):
        """Runs parse_cmd but adds settings to the keyword args"""
        return parse_cmd(s,settings=settings,**kwargs)
    
    s = r"""
    {settings.bwa_path} aln {settings.bwa_reference_fasta_path} {fastq} > {output_sai}
    """
    return _parse_cmd(s,fastq=fastq,output_sai=output_sai)

class BWA_Sampe(step.Step):
    outputs = {'sam':'raw.sam'}
    mem_req=5000
    
    def many2one_cmd(self,input_nodes,tags):
        node_r1 = input_nodes[0]
        node_r2 = input_nodes[1]
        return (
            r"""
                {settings.bwa_path} sampe \
                -f  {{output_dir}}/{{outputs[sam]}} \
                -r "@RG\tID:{node_r1.tags[RG_ID]}\tLB:{node_r1.tags[RG_LIB]}\tSM:{node_r1.tags[sample]}\tPL:{node_r1.tags[RG_PLATFORM]}" \
                {settings.bwa_reference_fasta_path} \
                {node_r1.output_paths[sai]} \
                {node_r2.output_paths[sai]} \
                {node_r1.tags[fq_path]} \
                {node_r2.tags[fq_path]}
            """,
            {'node_r1': node_r1,
             'node_r2': node_r1 }
        )

class CleanSam(step.Step):
    outputs = {'bam':'cleaned.bam'}
    mem_req = 3500
    
    def one2one_cmd(self,input_node,input_type='bam'):
        return (
            r"""
                {Picard_cmd} \
                I= {input} \
                O={{output_dir}}/{{outputs[bam]}}
            """,
            {'Picard_cmd':get_Picard_cmd('CleanSam.jar'),
             'input':input_node.output_paths[input_type]}
        )

class MergeSamFiles(step.Step):
    "Merges and Sorts"
    outputs = {'bam':'merged.bam'}
    mem_req = 5000
    
    def many2one_cmd(self,input_nodes,tags,assume_sorted=True):
        INPUTs = " \\\n".join(["INPUT={0}".format(n.output_paths['bam']) for n in input_nodes])
        return (
            r"""
                {Picard_cmd} \
                {INPUTs} \
                OUTPUT={{output_dir}}/{{outputs[bam]}} \
                SORT_ORDER=coordinate \
                MERGE_SEQUENCE_DICTIONARIES=True \
                ASSUME_SORTED={assume_sorted}
            """,
            {'Picard_cmd':get_Picard_cmd('MergeSamFiles.jar'),
             'INPUTs':INPUTs,
             'assume_sorted':assume_sorted
             }
        )

class MarkDuplicates(step.Step):
    outputs = {'bam':'deduped.bam',
               'metrics_file':'metrics.log'}
    mem_req = 3000
    
    def one2one_cmd(self,input_node,assume_sorted=True):
        return (
            r"""
                {Picard_cmd} \
                I={input_node.output_paths[bam]} \
                O={{output_dir}}/{{outputs[bam]}} \
                METRICS_FILE={{output_dir}}/{{outputs[metrics_file]}} \
                ASSUME_SORTED={assume_sorted}
            """,
            {'Picard_cmd':get_Picard_cmd('MarkDuplicates.jar'),
             'assume_sorted':assume_sorted}
        )
    
class BuildBamIndex(step.Step):
    mem_req = 3500
    
    def one2one_cmd(self,input_node):
        return (
            r"""
                {Picard_cmd} \
                INPUT={input_node.output_paths[bam]} \
                OUTPUT={input_node.output_paths[bam]}.bai
            """,
            {'Picard_cmd':get_Picard_cmd('BuildBamIndex.jar')}
        )

class RealignerTargetCreator(step.Step):
    outputs = {'intervals':'target.intervals'}
    mem_req = 3000
    
    def many2many_cmd(self,input_batch):
        """
        Used to generate the knowns only interval list.  Just ignore all the input_batch.  Although this is a many2many, its really a many2one
        """
        yield (
            r"""
                {settings.GATK_cmd} \
                -T RealignerTargetCreator \
                -R {settings.reference_fasta_path} \
                -o {{output_dir}}/{{outputs[intervals]}} \
                --known {settings.indels_1000g_phase1_path} \
                --known {settings.mills_path}
            """,
            {},
            {'mode':'KNOWNS_ONLY'}
         )    
    
    def one2one_cmd(self,input_node):
        return (
            r"""
                {settings.GATK_cmd} \
                -T RealignerTargetCreator \
                -R {settings.reference_fasta_path} \
                -I {input_node.output_paths[bam]} \
                -o {{output_dir}}/{{outputs[intervals]}} \
                --known {settings.indels_1000g_phase1_path} \
                --known {settings.mills_path}
            """,
            {}
        )

class IndelRealigner(step.Step):
    outputs = {'bam':'realigned.bam'}
    mem_req = 1500
    
    def one2one_cmd(self,input_node,rtc_batch,model='USE_READS'):
        """
        
        :param model: USE_READS or KNOWNS_ONLY
        """
        if rtc_batch == None:
            intervals = '/dev/null'
        else:
            rtc_node = rtc_batch.get_node_by(input_node.tags)
            if model=='USE_READS':
                intervals = rtc_node.output_paths['intervals']
            elif model == 'KNOWNS_ONLY':
                intervals = rtc_batch.nodes[0].output_paths['intervals'] #in knowns only, there's just one node in the RTC step
        return (
            r"""
                {settings.GATK_cmd} \
                -T IndelRealigner \
                -R {settings.reference_fasta_path} \
                -I {input_node.output_paths[bam]} \
                -o {{output_dir}}/{{outputs[bam]}} \
                -targetIntervals {intervals} \
                -known {settings.indels_1000g_phase1_path} \
                -known {settings.mills_path} \
                -model {model}
            """,
            {'model':model,
             'intervals':intervals}
        )

class BaseQualityScoreRecalibration(step.Step):
    outputs = {'recal':'bqsr.recal'}
    mem_req = 2000
    
    def one2one_cmd(self,input_node):
        return (
            r"""
                {settings.GATK_cmd} \
                -T BaseRecalibrator \
                -R {settings.reference_fasta_path} \
                -I {input_node.output_paths[bam]} \
                -o {{output_dir}}/{{outputs[recal]}} \
                -knownSites {settings.indels_1000g_phase1_path} \
                -knownSites {settings.mills_path} \
                --disable_indel_quals \
                -cov ReadGroupCovariate \
                -cov QualityScoreCovariate \
                -cov CycleCovariate \
                -cov ContextCovariate
            """,
            {}
        )

class PrintReads(step.Step):
    outputs = {'bam':'recalibrated.bam'}
    mem_req = 3000
    
    def one2one_cmd(self,input_node,bqsr_batch):
        bqsr_node = bqsr_batch.get_node_by(*input_node.tags)
        return (
            r"""
                {settings.GATK_cmd} \
                -T PrintReads \
                -R {settings.reference_fasta_path} \
                -I {input_node.output_paths[bam]} \
                -o {{output_dir}}/{{outputs[bam]}}  \
                -BQSR {{bqsr_node[recal]}}
            """,
            {}
        )
    
class UnifiedGenotyper(step.Step):
    outputs = {'vcf':'raw.vcf'}
    mem_req = 3000
    
    def many2many_cmd(self,input_batch,intervals):
        """
        :param input_batch: sample level bams
        """
        input_bams = ' '.join([ '-I {0}'.format(n.output_paths['bam']) for n in input_batch.nodes ])
        for glm in ['SNP','INDEL']:
            for interval in intervals:
                yield (
                    r"""
                        {settings.GATK_cmd} \
                        -T UnifiedGenotyper \
                        -R {settings.reference_fasta_path} \
                        --dbsnp {settings.dbsnp_path} \
                        -glm {glm} \
                        {input_bams} \
                        -o {{output_dir}}/{{outputs[bam]} \
                        -A DepthOfCoverage \
                        -A HaplotypeScore \
                        -A InbreedingCoeff \
                        -baq CALCULATE_AS_NECESSARY \
                        -L {interval}
                    """,
                    {'input_bams':input_bams,
                     'interval':interval,
                     'glm':glm},
                    {'interval':interval,
                     'glm':glm}
                )


class CombineVariants(step.Step):
    outputs = {'vcf':'combined.vcf'}
    mem_req = 3000

    def many2one_cmd(input_nodes,genotypeMergeOptions='UNSORTED'):
        """
        :param genotypemergeoptions: select from the following
            UNIQUIFY - Make all sample genotypes unique by file. Each sample shared across RODs gets named sample.ROD.
            PRIORITIZE - Take genotypes in priority order (see the priority argument).
            UNSORTED - Take the genotypes in any order.
            REQUIRE_UNIQUE - Require that all samples/genotypes be unique between all inputs.
        """
        INPUTs = " \\\n".join(["--variant {0}".format(n.output_paths['vcf']) for n in input_nodes])
        return (
                r"""
                    {settings.GATK_cmd} \
                    -T CombineVariants \
                    -R {settings.reference_fasta_path} \
                    {INPUTs} \
                    -o {{output_dir}}/{{outputs[bam]}} \
                    -genotypeMergeOptions {genotypeMergeOptions}
                """,
                {'INPUTs':INPUTs}
            )
        
class VariantQualityRecalibration(step.Step):
    outputs = {'recal':'vqr.recal',
               'tranches':'vqr.tranches',
               'rscript':'plot.R'
               }
    mem_req = 3000
    
    def one2one_cmd(self,input_node,exome_or_wgs,haplotypeCaller_or_unififedGenotyper='UnifiedGenotyper',inbreedingcoeff=True):
        """
        :param exome_or_wgs: choose from ('exome','wgs')
        :param haplotypeCaller_or_unifiedGenotyper: choose from ('Haplotypecaller','UnifiedGenotyper')
        :param inbreedingcoeff: True to use the inbreedingcoeff annotation, False to not use it
        
        ..note:: inbreedingcoeff can only be calculated if there are more than 20 samples
        """
        #validation
        mode = node.tags['mode']
        if mode not in ['SNP','INDEL']:
            raise ValidationError('invalid parameter')
        if exome_or_wgs not in ['exome','wgs']:
            raise ValidationError('invalid parameter')
        if haplotypeCaller_or_unifiedGenotyper not in ['HaplotypeCaller','UnifiedGenotyper']:
            raise ValidationError('invalid parameter')
        
        InbreedingCoeff = ''
        if inbreedingcoeff:
            InbreedingCoeff = '-an InbreedingCoeff'
        
        if exome_or_wgs == 'exome':
            if haplotypeCaller_or_unifiedGenotyper == 'UnifiedGenotyper':
                if mode == 'SNP': 
                    cmd = r"""
                    {settings.GATK_cmd} \
                    -T VariantRecalibrator \
                    -R {settings.reference_fasta_path} \
                    -input {input_vcf} \
                    --maxGaussians 6 \
                    -resource:hapmap,known=false,training=true,truth=true,prior=15.0 {settings.hapmap_path} \
                    -resource:omni,known=false,training=true,truth=false,prior=12.0 {settings.omni_path} \
                    -resource:dbsnp,known=true,training=false,truth=false,prior=6.0 {settings.dbsnp_path} \
                    -an QD -an HaplotypeScore -an MQRankSum -an ReadPosRankSum -an FS -an MQ {InbreedingCoeff} \
                    -mode SNP \
                    -recalFile {{output_dir}}/{{outputs[recal]}} \
                    -tranchesFile {{output_dir}}/{{outputs[tranches]}} \
                    -rscriptFile {{output_dir}}/{{outputs[rscript]}}
                    """
                elif mode == 'INDEL':
                    cmd = r"""
                    {settings.GATK_cmd} \
                    -T VariantRecalibrator \
                    -R {settings.reference_fasta_path} \
                    -input {input_vcf} \
                    --maxGaussians 4 -std 10.0 -percentBad 0.12 \
                    -resource:mills,known=true,training=true,truth=true,prior=12.0 {settings.mills_path} \
                    -an QD -an FS -an HaplotypeScore -an ReadPosRankSum -an {InbreedingCoeff} \
                    -mode INDEL \
                    -recalFile {{output_dir}}/{{outputs[recal]}} \
                    -tranchesFile {{output_dir}}/{{outputs[tranches]}} \
                    -rscriptFile {{output_dir}}/{{outputs[rscript]}}
                    """
            elif haplotypeCaller_or_unifiedGenotyper == 'HaplotypeCaller':
                raise NotImplementedError()
                if mode == 'SNP' or mode == 'INDEL': 
                    cmd = r"""
                    {settings.GATK_cmd} \
                    -T VariantRecalibrator \
                    -R {settings.reference_fasta_path} \
                    -input {input_vcf} \
                    --maxGaussians 6 \
                    -resource:hapmap,known=false,training=true,truth=true,prior=15.0 hapmap_3.3.b37.sites.vcf \
                    -resource:omni,known=false,training=true,truth=false,prior=12.0 1000G_omni2.5.b37.sites.vcf \
                    -resource:mills,known=true,training=true,truth=true,prior=12.0 Mills_and_1000G_gold_standard.indels.b37.sites.vcf \
                    -resource:dbsnp,known=true,training=false,truth=false,prior=6.0 dbsnp_135.b37.vcf \
                    -an QD -an MQRankSum -an ReadPosRankSum -an FS -an MQ -an InbreedingCoeff -an ClippingRankSum \
                    -mode BOTH \
                    -recalFile {{output_dir}}/{{outputs[recal]}} \
                    -tranchesFile {{output_dir}}/{{outputs[tranches]}} \
                    -rscriptFile {{output_dir}}/{{outputs[rscript]}}
                    """
        elif mode == 'wgs':
            raise NotImplementedError()
        
        return (
            cmd,
            {'input_vcf':input_node.output_paths['vcf']}
        )        

class ApplyRecalibration(step.Step):
    outputs = {'vcf':'recalibrated.vcf'}
    mem_req = 3000
    
    def one2one_cmd(self,input_node,vqr_batch,haplotypeCaller_or_unififedGenotyper='UnifiedGenotyper'):
        """
        
        :param input_node: the node with the raw vcf output files
        :param vqr_batch: the VariantQualityRecalibration batch
        :param haplotypeCaller_or_unifiedGenotyper: choose from ('Haplotypecaller','UnifiedGenotyper')
        
        ..note:: inbreedingcoeff is only calculated if there are more than 20 samples
        """
        #validation
        mode = input_node.tags
        if mode not in ['SNP','INDEL']:
            raise ValidationError('invalid parameter')
        if haplotypeCaller_or_unifiedGenotyper not in ['HaplotypeCaller','UnifiedGenotyper']:
            raise ValidationError('invalid parameter')
        
        if haplotypeCaller_or_unifiedGenotyper == 'UnifiedGenotyper':
            if mode == 'SNP': 
                cmd = r"""
                {settings.GATK_cmd} \
                -T ApplyRecalibration \
                -R {settings.reference_fasta_path} \
                -input {input_vcf} \
                -tranchesFile {input_tranches} \
                -recalFile {input_recal} \
                -o {{output_dir}}/{{outputs[vcf]}} \
                --ts_filter_level 99.0 \
                -mode SNP
                """
            elif mode == 'INDEL':
                cmd = r"""
                {settings.GATK_cmd} \
                -T ApplyRecalibration \
                -R {settings.reference_fasta_path} \
                -input {input_vcf} \
                -tranchesFile {input_tranches} \
                -recalFile {input_recal} \
                -o {{output_dir}}/{{outputs[vcf]}} \
                --ts_filter_level 95.0 \
                -mode INDEL
                """
        elif haplotypeCaller_or_unifiedGenotyper == 'HaplotypeCaller':
            raise NotImplementedError()
            if mode == 'SNP' or mode == 'INDEL': 
                cmd = r"""
                {settings.GATK_cmd} \
                -T ApplyRecalibration \
                -R {settings.reference_fasta_path} \
                -input {input_vcf} \
                -tranchesFile {input_tranches} \
                -recalFile {input_recal} \
                -o {{output_dir}}/{{outputs[vcf]}} \
                --ts_filter_level 97.0 \
                -mode BOTH
                """
        return (
            cmd,
            {'input_vcf':input_node.output_paths['vcf'],
             'input_tranches':input_node.output_paths['tranches']}
        )        

