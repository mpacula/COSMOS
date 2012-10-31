import cosmos.session
from cosmos.Workflow.models import Workflow
from cosmos.contrib import step
import steps
import os
import make_data_dict
import json

##make samples dictionary
samples=[]

if os.environ['COSMOS_SETTINGS_MODULE'] == 'config.gpp':
    WF = Workflow.start(name='GP 48Exomes with Simulated',default_queue='high_priority',restart=False)
    data_dict = json.loads(make_data_dict.main(input_dir='/nas/erik/ngs_data/48exomes',depth=2))
    simulated = [s for s in make_data_dict.yield_simulated_files(input_dir='/nas/erik/ngs_data/simulated')]
    data_dict += simulated
elif os.environ['COSMOS_SETTINGS_MODULE'] == 'config.orchestra':
#    WF = Workflow.start(name='GPP 48Exomes GATK i2b2',default_queue='i2b2_2h',restart=True)
#    data_dict = json.loads(make_data_dict.main(input_dir='/scratch/esg21/ngs_data/48exomes',depth=2))
    WF = Workflow.start(name='GPP 48Exomes GATK shared',default_queue='shared_2h',restart=False)
    data_dict = json.loads(make_data_dict.main(input_dir='/scratch/esg21/ngs_data/48exomes',depth=2))
    simulated = [s for s in make_data_dict.yield_simulated_files(input_dir='/scratch/esg21/ngs_data/simulated')]
    data_dict += simulated

#Enable steps
step.workflow = WF

### Alignment
bwa_aln = steps.BWA_Align("BWA Align").many2many(input_batch=None,data_dict=data_dict)
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
cv1 = steps.CombineVariants("Combine Variants").many2one(input_batch=ug,group_by=['glm'])
inbreeding_coeff = len(samples)> 19 #20 samples are required to use this annotation for vqr
vqr = steps.VariantQualityRecalibration("Variant Quality Recalibration").one2one(cv1,exome_or_wgs='exome',inbreeding_coeff=inbreeding_coeff)
ar = steps.ApplyRecalibration("Apply Recalibration").one2one(input_batch=cv1,vqr_batch=vqr)
cv2 = steps.CombineVariants("Combine Variants2").many2one(input_batch=ar,group_by=[])
    
WF.finished(delete_unused_batches=True)
