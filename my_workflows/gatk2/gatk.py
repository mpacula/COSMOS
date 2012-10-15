import cosmos_session
from Workflow.models import Workflow
import steps
from sample import Sample,Fastq
import os


WF = Workflow.start(name='GPP 48Exomes GATK2',restart=False)
steps.step.workflow = WF
assert isinstance(WF, Workflow)

##make samples dictionary
input_dir='/nas/erik/test_data'
samples=[]

#for pool_dir in os.listdir(input_dir):
#    for sample_dir in filter(lambda x: x!='.DS_Store',os.listdir(os.path.join(input_dir,pool_dir))):
#        samples.append(Sample.createFromPath(os.path.join(input_dir,pool_dir,sample_dir)))
for sample_dir in filter(lambda x: x!='.DS_Store',os.listdir(input_dir)):
    samples.append(Sample.createFromPath(os.path.join(input_dir,sample_dir)))


contigs = [str(x) for x in range(1,23)+['X','Y']] #list of chroms: [1,2,3,..X,Y]

### Alignment

bwa_aln = WF.add_batch("BWA Align")
if not bwa_aln.successful:
    for sample in samples:
        for fqp in sample.yield_fastq_pairs():
            for fq in fqp:
                bwa_aln.add_node(name = fq.filename,
                    pcmd = steps.bwa_aln(fastq=fq.path,
                                            output_sai='{output_dir}/{outputs[sai]}'),
                    outputs = {'sai':'align.sai'},
                    tags = {
                    'sample':sample.name,
                    'lane': fq.lane,
                    'fq_partNumber': fq.partNumber,
                    'fq_path': fq.path,
                    'RG_ID':'%s.L%s' % (sample.flowcell,fq.lane),
                    'RG_LIB':'LIB-%s' % sample.name,
                    'RG_PLATFORM':'ILLUMINA',
                },
                mem_req=3500)
    WF.run_wait(bwa_aln)

bwa_sampe = steps.BWA_Sampe("BWA Sampe").many2one(input_batch=bwa_aln,group_by=['sample','lane','fq_partNumber'])
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
realigned_samples = steps.IndelRealigner("IndelRealigner Samples").one2one(input_batch=deduped_lane,rtc_batch=rtc_samples,model='USE_READS')

# Variant Calling

    
    
WF.finished(delete_unused_batches=True)
