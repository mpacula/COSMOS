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
                {Picard_CleanSam} \
                I= \
                O={{output_dir}}/{{outputs[bam]}}
            """,
            {'Picard_CleanSam':get_Picard_cmd('CleanSam.jar'),
             'input':input_node.output_paths[input_type]}
        )

#class SortSam(step.Step):
#    outputs = {'bam':'sorted.bam'}
#    mem_req = 3500
#    
#    def one2one_cmd(self,input_node):
#        return (
#            r"""
#                {Picard_SortSam} \
#                I={input_node.output_paths[bam]} \
#                O={{output_dir}}/{{outputs[bam]}} \
#                SORT_ORDER=coordinate
#            """,
#            {'Picard_SortSam':get_Picard_cmd('SortSam.jar')}
#        )

class MergeSamFiles(step.Step):
    "Merges and Sorts"
    outputs = {'bam':'merged.bam'}
    mem_req = 5000
    
    def many2one_cmd(self,input_nodes,tags,assume_sorted=True):
        INPUTs = " \\\n".join(["INPUT={0}".format(b) for b in input_bams])
        return (
            r"""
                {Picard_MergeSamFiles} \
                {INPUTs} \
                OUTPUT={{output_dir}}/{{outputs[bam]}} \
                SORT_ORDER=coordinate \
                MERGE_SEQUENCE_DICTIONARIES=True \
                ASSUME_SORTED={assume_sorted}
            """,
            {'Picard_MergeSamFiles':get_Picard_cmd('MergeSamFiles.jar'),
             'INPUTs':INPUTs,
             'assume_sorted':assume_sorted
             }
        )

class BuildBamIndex(step.Step):
    mem_req = 3500
    
    def one2one_cmd(self,input_node):
        return (
            r"""
                {Picard_BuildBamIndex} \
                INPUT={input_bam} \
                OUTPUT={output_bai}
                O={input_node.outputs[bam]}
            """,
            {'Picard_BuildBamIndex':get_Picard_cmd('BuildBamIndex.jar')}
        )

class RealignerTargetCreator(step.Step):
    outputs = {'intervals':'target.intervals'}
    mem_req = 2000
    
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
    mem_req = 2000
    
    def one2one_cmd(self,input_node,realignerTargetCreator_node,model='USE_READS'):
        return (
            r"""
                {settings.GATK_cmd} \
                -T IndelRealigner \
                -R {settings.reference_fasta_path} \
                -I {input_node.output_paths[bam]} \
                -targetIntervals {targetIntervals} \
                -o {{output_dir}}/{{outputs[intervals]}} \
                -known {settings.indels_1000g_phase1_path} \
                -known {settings.mills_path} \
                -model {model}
            """,
            {'model':model}
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
                -o {{output_dir}}/{{outputs[recal_report]}} \
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
    mem_req = 2000
    
    def one2one_cmd(self,input_node,bqsr_node):
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

class PrintReads(step.Step):
    outputs = {'bam':'recalibrated.bam'}
    mem_req = 2000
    
    def one2many_cmd(self,input_node,contigs):
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

def UnifiedGenotyper(input_bams,output_bam,interval,glm):
    input_bams = ' '.join([ '-I {0}'.format(ib) for ib in input_bams ])
    s = r"""
    {settings.GATK_cmd} \
    -T UnifiedGenotyper \
    -R {settings.reference_fasta_path} \
    --dbsnp {settings.dbsnp_path} \
    -glm {glm} \
    {input_bams} \
    -o {output_bam} \
    -A DepthOfCoverage \
    -A HaplotypeScore \
    -A InbreedingCoeff \
    -baq CALCULATE_AS_NECESSARY \
    -L {interval}
    """ 
    return _parse_cmd(s,input_bams=input_bams,output_bam=output_bam,interval=interval,glm=glm)


def CombineVariants(input_vcfs,output_vcf,genotypeMergeOptions):
    """
    :param input_vcfs: a list of paths to variants
    :param: genotypemergeoptions: select from the following
        UNIQUIFY - Make all sample genotypes unique by file. Each sample shared across RODs gets named sample.ROD.
        PRIORITIZE - Take genotypes in priority order (see the priority argument).
        UNSORTED - Take the genotypes in any order.
        REQUIRE_UNIQUE - Require that all samples/genotypes be unique between all inputs.
    """
    #INPUTs = " \\\n".join(["--variant:{0},VCF {1}".format(vcf[0],vcf[1]) for vcf in input_vcfs])
    INPUTs = " \\\n".join(["--variant {0}".format(vcf) for vcf in input_vcfs])
    s = r"""
    {settings.GATK_cmd} \
    -T CombineVariants \
    -R {settings.reference_fasta_path} \
    {INPUTs} \
    -o {output_vcf} \
    -genotypeMergeOptions {genotypeMergeOptions}
    """ 
    return _parse_cmd(s,INPUTs=INPUTs,output_vcf=output_vcf,genotypeMergeOptions=genotypeMergeOptions)

def VariantQualityRecalibration(input_vcf,output_recal,output_tranches,output_rscript,mode,exome_or_wgs,haplotypeCaller_or_unifiedGenotyper,inbreedingcoeff=True):
    """
    note that inbreedingcoeff is only calculated if there were more than 20 samples
    mode should probably be SNP or INDEL
    """
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
                s = r"""
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
                -recalFile {output_recal} \
                -tranchesFile {output_tranches} \
                -rscriptFile {output_rscript}
                """
            elif mode == 'INDEL':
                s = r"""
                {settings.GATK_cmd} \
                -T VariantRecalibrator \
                -R {settings.reference_fasta_path} \
                -input {input_vcf} \
                --maxGaussians 4 -std 10.0 -percentBad 0.12 \
                -resource:mills,known=true,training=true,truth=true,prior=12.0 {settings.mills_path} \
                -an QD -an FS -an HaplotypeScore -an ReadPosRankSum -an {InbreedingCoeff} \
                -mode INDEL \
                -recalFile {output_recal} \
                -tranchesFile {output_tranches} \
                -rscriptFile {output_rscript}
                """
#        elif haplotypeCaller_or_unifiedGenotyper == 'HaplotypeCaller':
#            raise NotImplementedError()
#            if mode == 'SNP' or mode == 'INDEL': 
#                s = r"""
#                {settings.GATK_cmd} \
#                -T VariantRecalibrator \
#                -R {settings.reference_fasta_path} \
#                -input {input_vcf} \
#                --maxGaussians 6 \
#                -resource:hapmap,known=false,training=true,truth=true,prior=15.0 hapmap_3.3.b37.sites.vcf \
#                -resource:omni,known=false,training=true,truth=false,prior=12.0 1000G_omni2.5.b37.sites.vcf \
#                -resource:mills,known=true,training=true,truth=true,prior=12.0 Mills_and_1000G_gold_standard.indels.b37.sites.vcf \
#                -resource:dbsnp,known=true,training=false,truth=false,prior=6.0 dbsnp_135.b37.vcf \
#                -an QD -an MQRankSum -an ReadPosRankSum -an FS -an MQ -an InbreedingCoeff -an ClippingRankSum \
#                -mode BOTH \
#                """
    elif mode == 'wgs':
        raise NotImplementedError()
            
    return _parse_cmd(s,input_vcf=input_vcf,output_recal=output_recal,InbreedingCoeff=InbreedingCoeff,output_tranches=output_tranches,output_rscript=output_rscript)

def ApplyRecalibration(input_vcf,input_recal,input_tranches,output_recalibrated_vcf,mode,haplotypeCaller_or_unifiedGenotyper):
    """
    """
    if mode not in ['SNP','INDEL']:
        raise ValidationError('invalid parameter')
    if haplotypeCaller_or_unifiedGenotyper not in ['HaplotypeCaller','UnifiedGenotyper']:
        raise ValidationError('invalid parameter')
    
    if haplotypeCaller_or_unifiedGenotyper == 'UnifiedGenotyper':
        if mode == 'SNP': 
            s = r"""
            {settings.GATK_cmd} \
            -T ApplyRecalibration \
            -R {settings.reference_fasta_path} \
            -input {input_vcf} \
            -tranchesFile {input_tranches} \
            -recalFile {input_recal} \
            -o {output_recalibrated_vcf} \
            --ts_filter_level 99.0 \
            -mode SNP
            """
        elif mode == 'INDEL':
            s = r"""
            {settings.GATK_cmd} \
            -T ApplyRecalibration \
            -R {settings.reference_fasta_path} \
            -input {input_vcf} \
            -tranchesFile {input_tranches} \
            -recalFile {input_recal} \
            -o {output_recalibrated_vcf} \
            --ts_filter_level 95.0 \
            -mode INDEL
            """
    elif haplotypeCaller_or_unifiedGenotyper == 'HaplotypeCaller':
        raise NotImplementedError()
        if mode == 'SNP' or mode == 'INDEL': 
            s = r"""
            {settings.GATK_cmd} \
            -T ApplyRecalibration \
            -R {settings.reference_fasta_path} \
            -input {input_vcf} \
            -tranchesFile {input_tranches} \
            -recalFile {input_recal} \
            -o {output_recalibrated_vcf} \
            --ts_filter_level 97.0 \
            -mode BOTH
            """
            
    return _parse_cmd(s,input_vcf=input_vcf,output_recalibrated_vcf=output_recalibrated_vcf,input_recal=input_recal,input_tranches=input_tranches)
