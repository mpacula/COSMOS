import cosmos_session
from Workflow.models import Workflow
from Cosmos.addons import step
import steps
from sample import Sample,Fastq
import os



##make samples dictionary
samples=[]

if os.environ['COSMOS_SETTINGS_MODULE'] == 'config.gpp':
    WF = Workflow.start(name='GPP 48Exomes GATK3',default_queue='high_priority',restart=False)
    
    input_dir='/nas/erik/test_data'
    for sample_dir in filter(lambda x: x!='.DS_Store',os.listdir(input_dir)):
        samples.append(Sample.createFromPath(os.path.join(input_dir,sample_dir)))
    #input_dir='/scratch/esg21/projects/48exomes/'
    #for pool_dir in os.listdir(input_dir):
    #    for sample_dir in filter(lambda x: x!='.DS_Store',os.listdir(os.path.join(input_dir,pool_dir))):
    #       samples.append(Sample.createFromPath(os.path.join(input_dir,pool_dir,sample_dir)))
    #     
elif os.environ['COSMOS_SETTINGS_MODULE'] == 'config.orchestra':
    WF = Workflow.start(name='GPP 48Exomes GATK3 Test',default_queue='i2b2_2h',restart=False)
    
    input_dir='/scratch/esg21/test_data'
    for sample_dir in filter(lambda x: x!='.DS_Store',os.listdir(input_dir)):
        samples.append(Sample.createFromPath(os.path.join(input_dir,sample_dir)))

#Enable steps
step.workflow = WF

### Alignment
bwa_aln = steps.BWA_Align("BWA Align").many2many(input_batch=None,samples=samples)
bwa_sampe = steps.BWA_Sampe("BWA Sampe").many2one(input_batch=bwa_aln,group_by=['sample','lane','fq_chunk'])
clean_bams = steps.CleanSam("Clean Bams").one2one(input_batch=bwa_sampe,input_type='sam')

"""
Following the "Best" GATK practices for deduping, realignment, and bqsr
for each sample
    lanes.bam <- merged lane.bams for sample
    dedup.bam <- MarkDuplicates(lanes.bam)
    realigned.bam <- realign(dedup.bam) [with known sites included if available]
    recal.bam <- recal(realigned.bam)
    sample.bam <- recal.bam
"""
contigs = [str(x) for x in range(1,23)+['X','Y']] #list of chroms: [1,2,3,..X,Y]

sample_bams = steps.MergeSamFiles("Merge Bams by Sample").many2one(input_batch=clean_bams,group_by=['sample'],assume_sorted=False)
deduped_by_samples = steps.MarkDuplicates("Mark Duplicates in Samples").one2one(input_batch=sample_bams)
index_samples = steps.BuildBamIndex("Index Deduped Samples").one2one(input_batch=deduped_by_samples)
rtc_by_sample_chr = steps.RealignerTargetCreator("RealignerTargetCreator by Sample Chr").one2many(input_batch=deduped_by_samples,intervals=contigs)
realigned_by_sample_chr = steps.IndelRealigner("IndelRealigner by Sample Chr").one2many(input_batch=deduped_by_samples,rtc_batch=rtc_by_sample_chr,intervals=contigs,model='USE_READS')
bqsr_by_sample = steps.BaseQualityScoreRecalibration("Base Quality Score Recalibration by Sample").many2one(input_batch=realigned_by_sample_chr,group_by=['sample'])
recalibrated_samples = steps.PrintReads("Apply BQSR").many2one(input_batch=realigned_by_sample_chr,bqsr_batch=bqsr_by_sample,group_by=['sample'])

# Variant Calling
ug = steps.UnifiedGenotyper("Unified Genotyper").many2many(input_batch=recalibrated_samples,intervals=contigs)
cv1 = steps.CombineVariants("Combine Variants",hard_reset=True).many2one(input_batch=ug,group_by=['glm'])
inbreeding_coeff = len(samples)> 19 #20 samples are required to use this annotation for vqr
vqr = steps.VariantQualityRecalibration("Variant Quality Recalibration").one2one(cv1,exome_or_wgs='exome',inbreeding_coeff=inbreeding_coeff)
ar = steps.ApplyRecalibration("Apply Recalibration").one2one(input_batch=cv1,vqr_batch=vqr)
cv2 = steps.CombineVariants("Combine Variants2").many2one(input_batch=ar,group_by=[])
    
WF.finished(delete_unused_batches=True)
