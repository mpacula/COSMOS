from django.core.exceptions import ValidationError

import cosmos_session
from Cosmos.helpers import parse_command_string
import logging
import settings
from settings import get_Gatk_cmd,get_Picard_cmd

def _parse_cmd_str(s,**kwargs):
    return parse_command_string(s,settings=settings,**kwargs)

def bwa_aln(fastq,output_sai):
    s = r"""
    {settings.bwa_path} aln {settings.reference_fasta_path} {fastq} > {output_sai}
    """
    return _parse_cmd_str(s,fastq=fastq,output_sai=output_sai)


def bwa_sampe(output_sam,r1_sai,r2_sai,r1_fq,r2_fq,ID,LIBRARY,SAMPLE_NAME,PLATFORM='ILLUMINA'):
    s = r"""
    {settings.bwa_path} sampe \
    -f {output_sam} \
    -r "@RG\tID:{ID}\tLB:{LIBRARY}\tSM:{SAMPLE_NAME}\tPL:{PLATFORM}" \
    {settings.reference_fasta_path} \
    {r1_sai} \
    {r2_sai} \
    {r1_fq} \
    {r2_fq}
    """
    return _parse_cmd_str(s,output_sam=output_sam,r1_sai=r1_sai,r2_sai=r2_sai,r1_fq=r1_fq,r2_fq=r2_fq,ID=ID,LIBRARY=LIBRARY,SAMPLE_NAME=SAMPLE_NAME,PLATFORM=PLATFORM)

def create_bam_list():
    """bam.list input to queue"""
    s= r"""
    java \
    """
    return _parse_cmd_str(s,)

def queue(input_bam_list):
    s= r"""
    java \
    -classpath $CLASSPATH \
    -Xmx4g \
    -Djava.io.tmpdir=/nas/erik/tmp \
    -jar $QUEUE_DIR/QueueLite.jar \
    -S ~/workspace/Cosmos/my_workflows/gatk/DataProcessingPipeline.scala \
    -i $INPUT_FILE \
    -R $BUNDLE_DIR/human_g1k_v37.fasta \
    -D $BUNDLE_DIR/dbsnp_135.b37.vcf \
    -outputDir $OUTPUT_DIR/ \
    -p MGH_BC \
    -gv $OUTPUT_DIR/graphviz.dot \
    -gvsg $OUTPUT_DIR/graphviz_scatter_gather.dot \
    -log $OUTPUT_DIR/queue_output.log \
    -jobReport $OUTPUT_DIR/job_report.pdf \
    -retry 5 \
    -resMemReq 3 \
    -run
    """
    return _parse_cmd_str(s,)

def CleanSam(input,output):
     # remove alignments off the end of a contig (bwa concatenates all the reference contigs together)
    s = r"""
    {Picard_CleanSam} \
    I={input} \
    O={output}
    """
    return _parse_cmd_str(s,input=input,output=output,Picard_CleanSam=get_Picard_cmd('CleanSam.jar'))
    

def ReduceBam(input_bam,output_bam,interval):
    s = r"""
    {settings.GATK_cmd} \
    -R {settings.reference_fasta_path} \
    -T ReduceReads \
    -I {input_bam} \
    -o {output_bam} \
    -L {interval}
    """
    return _parse_cmd_str(s,input_bam=input_bam,output_bam=output_bam,interval=interval)

#i want input_bams to be a list so its not a command line argument right now
def MergeSamFiles(input_bams,output_bam,assume_sorted=True):
    INPUTs = " \\\n".join(["INPUT={0}".format(b) for b in input_bams])
    s = r"""
    {Picard_MergeSamFiles} \
    {INPUTs} \
    OUTPUT={output_bam} \
    SORT_ORDER=coordinate \
    MERGE_SEQUENCE_DICTIONARIES=True \
    ASSUME_SORTED={assume_sorted}
    """
    return _parse_cmd_str(s,INPUTs=INPUTs,output_bam=output_bam,assume_sorted=assume_sorted,Picard_MergeSamFiles=get_Picard_cmd('MergeSamFiles.jar'))

def HaplotypeCaller(input_bam,output_bam,interval,glm):
    pass

def _UnifiedGenotyper(input_bam,output_bam,interval,glm):
    """
    need to make variant annotation a separate step since some annotations use multi-sample info
    """
    s = r"""
    {settings.GATK_cmd} \
    -T UnifiedGenotyper \
    -R {settings.reference_fasta_path} \
    --dbsnp {settings.dbsnp_path} \
    -glm {glm} \
    -I {input_bam} \
    -o {output_bam} \
    -A DepthOfCoverage \
    -A HaplotypeScore \
    -A InbreedingCoeff \
    -baq CALCULATE_AS_NECESSARY \
    -L {interval}
    """ 
    return _parse_cmd_str(s,input_bam=input_bam,output_bam=output_bam,interval=interval,glm=glm)

def UnifiedGenotyper_SNP(input_bam,output_bam,interval):
    return _UnifiedGenotyper(input_bam=input_bam, output_bam=output_bam, interval=interval, glm='SNP')

def UnifiedGenotyper_INDEL(input_bam,output_bam,interval):
    return _UnifiedGenotyper(input_bam=input_bam, output_bam=output_bam, interval=interval, glm='INDEL')

def CombineVariants(input_vcfs,output_vcf,genotypeMergeOptions):
    """
    :param input_vcfs: a list of (sample.name,file_path_to_vcf) tuples
    :param: genotypemergeoptions: select from the following
        UNIQUIFY - Make all sample genotypes unique by file. Each sample shared across RODs gets named sample.ROD.
        PRIORITIZE - Take genotypes in priority order (see the priority argument).
        UNSORTED - Take the genotypes in any order.
        REQUIRE_UNIQUE - Require that all samples/genotypes be unique between all inputs.
    """
    INPUTs = " \\\n".join(["--variant:{0},VCF {1}".format(vcf[0],vcf[1]) for vcf in input_vcfs])
    s = r"""
    {settings.GATK_cmd} \
    -T CombineVariants \
    -R {settings.reference_fasta_path} \
    {INPUTs} \
    -o {output_vcf} \
    -genotypeMergeOptions {genotypeMergeOptions}
    """ 
    return _parse_cmd_str(s,INPUTs=INPUTs,output_vcf=output_vcf,genotypeMergeOptions=genotypeMergeOptions)

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
        elif haplotypeCaller_or_unifiedGenotyper == 'HaplotypeCaller':
            raise NotImplementedError()
            if mode == 'SNP' or mode == 'INDEL': 
                s = r"""
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
                """
    elif mode == 'wgs':
        raise NotImplementedError()
            
    return _parse_cmd_str(s,input_vcf=input_vcf,output_recal=output_recal,InbreedingCoeff=InbreedingCoeff,output_tranches=output_tranches,output_rscript=output_rscript)

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
            
    return _parse_cmd_str(s,input_vcf=input_vcf,output_recalibrated_vcf=output_recalibrated_vcf,input_recal=input_recal,input_tranches=input_tranches)
