import cosmos_session
from Workflow.models import Workflow
import commands
from sample import Sample,Fastq
import os


##make samples dictionary
input_dir='/nas/erik/48exomes'
samples=[]

for pool_dir in os.listdir(input_dir):
    for sample_dir in filter(lambda x: x!='.DS_Store',os.listdir(os.path.join(input_dir,pool_dir))):
        samples.append(Sample.createFromPath(os.path.join(input_dir,pool_dir,sample_dir)))

WF = Workflow.start(name='GPP 48Exomes GATK',restart=False)
#WF = Workflow.start(name='GATK Test2')
assert isinstance(WF, Workflow)

contigs = [str(x) for x in range(1,23)+['X','Y']] #list of chroms: [1,2,3,..X,Y]

### Alignment

B_bwa_aln = WF.add_batch("BWA Align")
if not B_bwa_aln.successful:
    for sample in samples:
        for fqp in sample.yield_fastq_pairs():
            for fq in fqp:
                B_bwa_aln.add_node(name = fq.filename,
                    pcmd = commands.bwa_aln(fastq=fq.path,
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
    WF.run_wait(B_bwa_aln) 

B_bwa_sampe = WF.add_batch("BWA Sampe")
if not B_bwa_sampe.successful:
    for tags,input_nodes in B_bwa_aln.group_nodes_by('sample','lane','fq_partNumber'):
        node_r1 = input_nodes[0]
        node_r2 = input_nodes[1]
        n = input_nodes[0]
        name = '{tags[sample]} {tags[lane]} {tags[fq_partNumber]}'.format(tags=n.tags) #can take out r1 and r2
        B_bwa_sampe.add_node(name = name,
                            pcmd = commands.bwa_sampe(r1_sai=node_r1.output_paths['sai'],
                                                      r2_sai=node_r2.output_paths['sai'],
                                                      r1_fq=node_r1.tags['fq_path'],
                                                      r2_fq=node_r2.tags['fq_path'],
                                                      ID=n.tags['RG_ID'],
                                                      LIBRARY=n.tags['RG_LIB'],
                                                      SAMPLE_NAME=n.tags['sample'],
                                                      PLATFORM=n.tags['RG_PLATFORM'],
                                                      output_sam='{output_dir}/{outputs[sam]}'),
                            outputs = {'sam':'sampe.sam'},
                            tags = tags,
                            mem_req=5000)
    WF.run_wait(B_bwa_sampe)

B_clean_sam = WF.add_batch("Clean Bams")
if not B_clean_sam.successful:
    for n in B_bwa_sampe.nodes:
        name = '{tags[sample]} L{tags[lane]} PN{tags[fq_partNumber]}'.format(tags=n.tags)
        B_clean_sam.add_node(name=name,
                             pcmd = commands.CleanSam(input_bam=n.output_paths['sam'],
                                                      output_bam='{output_dir}/{outputs[bam]}'),
                             outputs = {'bam':'cleaned.bam'},
                             tags = n.tags,
                             mem_req=3000)
    WF.run_wait(B_clean_sam)
    
B_sort_bams = WF.add_batch("Sort Bams")
if not B_sort_bams.successful:
    for n in B_clean_sam.nodes:
        name = n.name
        B_sort_bams.add_node(name=name,
                             pcmd = commands.SortSam(input_bam=n.output_paths['bam'],
                                                      output_bam='{output_dir}/{outputs[bam]}'),
                             outputs = {'bam':'sorted.bam'},
                             tags = n.tags,
                             mem_req=3000)
    WF.run_wait(B_sort_bams)    

#index
B_index = WF.add_batch("Index Sorted Bams")
if not B_index.successful:
    for n in B_sort_bams.nodes:
        B_index.add_node(name=n.name,
                       pcmd = commands.BuildBamIndex(input_bam=n.output_paths['bam'],
                                                     output_bai=n.output_paths['bam']+'.bai'),
                       tags = n.tags,
                       mem_req=3000)
    WF.run_wait(B_index)
                                            
### INDEL Realignment    
 
B_RTC = WF.add_batch("Realigner Target Creator")
if not B_RTC.successful:
    for n in B_sort_bams.nodes:
        B_RTC.add_node(name=n.name,
                       pcmd = commands.RealignerTargetCreator(input_bam=n.output_paths['bam'],
                                                              output_recal_intervals='{output_dir}/{outputs[targetIntervals]}'
                                                              ),
                       outputs = {'targetIntervals':'list.intervals'},
                       tags = n.tags,
                       mem_req=2250)
    WF.run_wait(B_RTC)
                                            
B_IR = WF.add_batch("Indel Realigner")
if not B_IR.successful:
    for n in B_sort_bams.nodes:
        B_IR.add_node(name=n.name,
                      pcmd = commands.IndelRealigner(input_bam=n.output_paths['bam'],
                                                    targetIntervals=B_RTC.get_node_by(**n.tags).output_paths['targetIntervals'],
                                                    output_bam='{output_dir}/{outputs[bam]}',
                                                    ),
                      outputs = {'bam':'realigned.bam'},
                      tags = n.tags,
                      mem_req=3000)
    WF.run_wait(B_IR)

### Base Quality Score Recalibration
     
B_BQSR = WF.add_batch("Base Quality Score Recalibration") #TODO test using SW
if not B_BQSR.successful:
    for n in B_IR.nodes:
        B_BQSR.add_node(name=n.name,
                        pcmd = commands.BaseQualityScoreRecalibration(input_bam = n.output_paths['bam'],
                                                                      output_recal_report='{output_dir}/{outputs[recal]}'),
                        outputs = {'recal':'bqsr.recal'},
                        tags = n.tags,
                        mem_req=3000)
    WF.run_wait(B_BQSR)
    
B_PR = WF.add_batch("Apply BQSR") #TODO test using SW
if not B_PR.successful:
    for n in B_IR.nodes:
        bqsr_node = B_BQSR.get_node_by(**n.tags)
        B_PR.add_node(name=n.name,
                      pcmd = commands.PrintReads(input_bam = n.output_paths['bam'],
                                                 output_bam = '{output_dir}/{outputs[bam]}',
                                                 input_recal_report=bqsr_node.output_paths['recal']),
                      outputs = {'bam':'recalibrated.bam'},
                      tags = n.tags,
                      mem_req=3000)
    WF.run_wait(B_PR) 


def MergeAndIndexBySample(input_batch,name1,name2):
    B_merge_bams = WF.add_batch(name1)
    if not B_merge_bams.successful:
        for tags,input_nodes in input_batch.group_nodes_by('sample'):
            sample_name = tags['sample']
            sample_sams = [ n.output_paths['bam'] for n in input_nodes ]
            B_merge_bams.add_node(name=sample_name,
                              pcmd = commands.MergeSamFiles(input_bams=sample_sams,
                                                            output_bam='{output_dir}/{outputs[bam]}',
                                                            assume_sorted=False),
                              outputs = {'bam':'{0}.bam'.format(sample_name)},
                              tags = tags,
                              mem_req=3000)
        WF.run_wait(B_merge_bams)
    
    B_index = WF.add_batch(name2)
    if not B_index.successful:
        for n in B_merge_bams.nodes:
            B_index.add_node(name=n.name,
                              pcmd = commands.BuildBamIndex(input_bam=n.output_paths['bam'],
                                                            output_bai='{outputs[bai]}'),
                              outputs = {'bai':'{0}.bai'.format(n.output_paths['bam'])},
                              tags = n.tags,
                              mem_req=3000)
        WF.run_wait(B_index)    
    return B_merge_bams, B_index

B_merge_bams, B_index = MergeAndIndexBySample(B_PR,"Merge Bams by Sample","Index Merged Bams")


### Genotype

B_UG = WF.add_batch(name="Unified Genotyper")
if not B_UG.successful:
    for tags,input_nodes in B_merge_bams.group_nodes_by('sample'):
        input_bam = input_nodes[0].output_paths['bam']
        sample_name = tags['sample']
        for chrom in contigs:
            for glm in ['INDEL','SNP']:
                B_UG.add_node(name='{0} {1} chr{2}'.format(sample_name,glm,chrom),
                              pcmd = commands.UnifiedGenotyper(input_bam=input_bam,
                                                            output_bam='{output_dir}/{outputs[vcf]}',
                                                            glm=glm,
                                                            interval=chrom),
                              outputs = {'vcf':'raw.vcf'.format(sample_name)},
                              tags = {'sample':sample_name,'chr':chrom, 'glm':glm},
                              mem_req=3000)
    WF.run_wait(B_UG)

# Merge VCFS

B_CV1 = WF.add_batch(name="Combine Variants into Sample VCFs")
if not B_CV1.successful:
    for tags,nodes_by_chr in B_UG.group_nodes_by('glm','sample'):
        sample_vcfs = [ (tags['sample'],n.output_paths['vcf']) for n in nodes_by_chr ]
        B_CV1.add_node(name="{tags[sample]} {tags[glm]}".format(tags=tags),
                      pcmd=commands.CombineVariants(input_vcfs=sample_vcfs,
                                                           output_vcf="{output_dir}/{outputs[vcf]}",
                                                           genotypeMergeOptions='REQUIRE_UNIQUE'),
                      tags=tags,
                      outputs={'vcf':'raw.vcf'},
                      mem_req=3000)
    WF.run_wait(B_CV1)

B_CV2 = WF.add_batch(name="Combine Variants into MasterVCF")
if not B_CV2.successful:
    for tags,input_nodes in B_CV1.group_nodes_by('glm'):
        glm_vcfs = [ (n.tags['sample'],n.output_paths['vcf']) for n in input_nodes ]
        B_CV2.add_node(name=tags['glm'],
                      pcmd=commands.CombineVariants(input_vcfs=glm_vcfs,
                                                   output_vcf="{output_dir}/{outputs[vcf]}",
                                                   genotypeMergeOptions='PRIORITIZE'),
                      tags=tags,
                      outputs={'vcf':'raw.vcf'},
                      mem_req=3000)
    WF.run_wait(B_CV2)


### Variant Recalibration

B_VQR = WF.add_batch(name="Variant Quality Recalibration")
if not B_VQR.successful:
    for n in B_CV2.nodes:
        input_vcf = n.output_paths['vcf']
        B_VQR.add_node(name=n.tags['glm'],
                       pcmd=commands.VariantQualityRecalibration(input_vcf=input_vcf,
                                                                inbreedingcoeff=False,
                                                                output_recal="{output_dir}/{outputs[recal]}",
                                                                output_tranches="{output_dir}/{outputs[tranches]}",
                                                                output_rscript="{output_dir}/{outputs[rscript]}",
                                                                mode=n.tags['glm'],
                                                                exome_or_wgs='exome',
                                                                haplotypeCaller_or_unifiedGenotyper='UnifiedGenotyper'),
                      tags=n.tags,
                      outputs={'recal':'output_bam.recal','tranches':'output_bam.tranches','rscript':'plot.R'},
                      mem_req=3000)
    WF.run_wait(B_VQR)

B_AR = WF.add_batch(name="Apply Recalibration")
if not B_AR.successful:
    for n in B_VQR.nodes:
        B_AR.add_node(name=n.name,
                      pcmd=commands.ApplyRecalibration(input_vcf=input_vcf,
                                                      input_recal=n.output_paths['recal'],
                                                      input_tranches=n.output_paths['tranches'],
                                                      output_recalibrated_vcf="{output_dir}/{outputs[vcf]}",
                                                      mode=n.tags['glm'],
                                                      haplotypeCaller_or_unifiedGenotyper='UnifiedGenotyper'),
                      tags=n.tags,
                      outputs={'vcf':'recalibrated.vcf'},
                      mem_req=3000)
    WF.run_wait(B_AR)
        
WF.finished(delete_unused_batches=True)
