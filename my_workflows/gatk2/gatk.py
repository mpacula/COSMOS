import cosmos_session
from Workflow.models import Workflow
from Cosmos.addons import step
import steps
from sample import Sample,Fastq
import os


WF = Workflow.start(name='GPP 48Exomes GATK2',restart=False)
step.workflow = WF
assert isinstance(WF, Workflow)

##make samples dictionary
samples=[]

input_dir='/nas/erik/test_data'
#input_dir='/scratch/esg21/test_data'
for sample_dir in filter(lambda x: x!='.DS_Store',os.listdir(input_dir)):
    samples.append(Sample.createFromPath(os.path.join(input_dir,sample_dir)))

#input_dir='/scratch/esg21/projects/48exomes/'
#for pool_dir in os.listdir(input_dir):
#    for sample_dir in filter(lambda x: x!='.DS_Store',os.listdir(os.path.join(input_dir,pool_dir))):
#        samples.append(Sample.createFromPath(os.path.join(input_dir,pool_dir,sample_dir)))


### Alignment
bwa_aln = steps.BWA_Align("BWA Align").many2many(input_batch=None,samples=samples)
bwa_sampe = steps.BWA_Sampe("BWA Sampe").many2one(input_batch=bwa_aln,group_by=['sample','lane','fq_chunk'])
clean_bams = steps.CleanSam("Clean Bams").one2one(input_batch=bwa_sampe,input_type='sam')

### Cleaning

"""
Following the "Best" GATK practices for deduping, realignment, and bqsr
1) for each lane.bam
       dedup.bam <- MarkDuplicate(lane.bam)
       realigned.bam <- realign(dedup.bam) [at only known sites, if possible, otherwise skip]
       recal.bam <- recal(realigned.bam)
    
2) for each sample
       recals.bam <- merged lane-level recal.bams for sample
       dedup.bam <- MarkDuplicates(recals.bam)
       realigned.bam <- realign(dedup.bam) [with known sites included if available]
       sample.bam <- realigned.bam
"""

# 1)
lane_bams = steps.MergeSamFiles("Merge Bams by Sample Lanes").many2one(input_batch=clean_bams,group_by=['sample','lane'],assume_sorted=False)
deduped_lanes = steps.MarkDuplicates("Mark Duplicates in Lanes").one2one(input_batch=lane_bams)
index_lanes = steps.BuildBamIndex("Index Deduped Lanes").one2one(input_batch=deduped_lanes)
rtc_lanes = steps.RealignerTargetCreator("RealignerTargetCreator for known INDELs").many2many(input_batch=deduped_lanes)
realigned_lanes = steps.IndelRealigner("IndelRealigner Lanes").one2one(input_batch=deduped_lanes,rtc_batch=rtc_lanes,model='KNOWNS_ONLY')
bqsr_lanes = steps.BaseQualityScoreRecalibration("BQSR Realigned Deduped Lanes").one2one(input_batch=realigned_lanes)
recaled_lanes = steps.PrintReads("Apply BQSR to Realigned Deduped Lanes").one2one(input_batch=realigned_lanes,bqsr_batch=bqsr_lanes)

# 2)
sample_bams = steps.MergeSamFiles("Merge Bams by Sample").many2one(input_batch=recaled_lanes,group_by=['sample'])
deduped_samples = steps.MarkDuplicates("Mark Duplicates in Samples").one2one(input_batch=sample_bams)
index_samples = steps.BuildBamIndex("Index Deduped Samples").one2one(input_batch=deduped_samples)
rtc_samples = steps.RealignerTargetCreator("RealignerTargetCreator Samples").one2one(input_batch=deduped_samples)
realigned_samples = steps.IndelRealigner("IndelRealigner Samples").one2one(input_batch=deduped_samples,rtc_batch=rtc_samples,model='USE_READS')

# Variant Calling
contigs = [str(x) for x in range(1,23)+['X','Y']] #list of chroms: [1,2,3,..X,Y]
ug = steps.UnifiedGenotyper("Unified Genotyper").many2many(input_batch=realigned_samples,intervals=contigs)
cv1 = steps.CombineVariants("Combine Variants",hard_reset=True).many2one(input_batch=ug,group_by=['glm'])
if len(samples) > 19:
    inbreeding_coeff = True
else:
    inbreeding_coeff = False
vqr = steps.VariantQualityRecalibration("Variant Quality Recalibration").one2one(cv1,exome_or_wgs='exome',inbreeding_coeff=inbreeding_coeff)
ar = steps.ApplyRecalibration("Apply Recalibration").one2one(input_batch=cv1,vqr_batch=vqr)
cv2 = steps.CombineVariants("Combine Variants2").many2one(input_batch=ar,group_by=[])
    
WF.finished(delete_unused_batches=True)
