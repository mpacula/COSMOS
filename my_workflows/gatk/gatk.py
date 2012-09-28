import cosmos_session
from Workflow.models import Workflow
import commands

WF = Workflow.resume(name='GPP_48Exomes_GATK',dry_run=False)
assert isinstance(WF, Workflow)

##make get samples dictionary
from samples import samples


### Alignment

B_bwa_aln = WF.add_batch("BWA Align")
if not B_bwa_aln.successful:
    for sample in samples:
        for fqp in sample.fastq_pairs:
            for fq in fqp.reads:
                B_bwa_aln.add_node(name = fq.name,
                    pcmd = commands.bwa_aln(fastq=fq.path,
                                            output_sai='{output_dir}/{outputs[sai]}'),
                    outputs = {'sai':'{0}.sai'.format(fqp.r1)},
                    tags = {
                    'sample':sample.name,
                    'lane': fqp.lane,
                    'readGroupNumber': fqp.readGroupNumber,
                    'pair_number' : fq.pairNum,
                    'fq_path': fq.path,
                    'RG_ID':'%s.L%s' % (sample.flowcell,fqp.lane),
                    'RG_LIB':'LIB-%s' % sample.name,
                    'RG_PLATFORM':'ILLUMINA',
                },
                mem_req=3000)
    WF.run_wait(B_bwa_aln) 

B_bwa_sampe = WF.add_batch("BWA Sampe")
if not B_bwa_aln.successful:
    for tags,input_nodes in B_bwa_aln.group_nodes_by('sample','lane','readGroupNumber'):
        node_r1 = input_nodes[0]
        node_r2 = input_nodes[0] 
        name = '{tags[sample]} {tags[lane]} {tags[readGroupNumber]} R1 and R2'.format(tags) #next time use this for name and output
        B_bwa_sampe.add_node(name = name,
                            pcmd = commands.bwa_sampe(r1_sai=node_r1.output_paths['sai'],
                                                      r2_sai=node_r2.output_paths['sai'],
                                                      r1_fq=node_r1.fq_path,
                                                      r2_fq=node_r2.fq_path,
                                                      ID=tags['RG_ID'],
                                                      LIBRARY=tags['RG_LIB'],
                                                      SAMPLE_NAME=tags['sample'],
                                                      PLATFORM=tags['RG_PLATFORM'],
                                                      output_sam='{output_dir}/{outputs[sam]}'),
                            outputs = {'sam':'{0}.sam'.format(name)},
                            tags = {
                                'sample':tags['sample'],
                                'lane': tags['lane'],
                                'readGroupNumber': tags['readGroupNumber'],
                            },
                            mem_req=3000)
    WF.run_wait(B_bwa_sampe)

B_clean_sam = WF.add_batch("Clean Bams")
if not B_clean_sam.successful:
    for n in B_bwa_sampe.nodes:
        name = '{tags[sample]} L{tags[lane]} RGN{tags[readGroupNumber]}'.format(tags=n.tags)
        B_clean_sam.add_node(name=name,
                             pcmd = commands.CleanSam(input=n.output_paths['sam'],
                                                      output='{output_dir}/{outputs[bam]}'),
                             outputs = {'bam':'cleaned.bam'},
                             tags = n.tags,
                             mem_req=1000)
    WF.run_wait(B_clean_sam)

B_merge1 = WF.add_batch("Merge Bams by Sample",hard_reset=False)
if not B_merge1.successful:
    for tags,input_nodes in B_clean_sam.group_nodes_by('sample'):
        sample_name = tags['sample']
        sample_sams = [ n.output_paths['bam'] for n in input_nodes ]
        B_merge1.add_node(name=sample_name,
                          pcmd = commands.MergeSamFiles(input_bams=sample_sams,
                                                        output_bam='{output_dir}/{outputs[bam]}',
                                                        assume_sorted=False),
                          outputs = {'bam':'{0}.bam'.format(sample_name)},
                          tags = {'sample':sample_name},
                          mem_req=1000)
    WF.run_wait(B_merge1)
                                            
### INDEL Realignment    
 
B_RTC = WF.add_batch("Realigner Target Creator")
if not B_RTC.successful:
    for tags,input_nodes in B_clean_sam.group_nodes_by('sample'):
        input_bam = input_nodes[0].output_paths['bam']
        sample_name = tags['sample']
        B_RTC.add_node(name=sample_name,
                       pcmd = commands.RealignerTargetCreator(input_bam=input_bam,
                                                     output_recal_intervals='{output_dir}/{outputs[recal]}'),
                       outputs = {'targetIntervals':'{0}.targetIntervals'.format(sample_name)},
                       tags = {'sample':sample_name},
                       mem_req=2000)
    WF.run_wait(B_RTC)

                                            
B_IR = WF.add_batch("Indel Realigner")
if not B_IR.successful:
    for tags,input_nodes in B_clean_sam.group_nodes_by('sample'):
        input_bam = input_nodes[0].output_paths['bam']
        sample_name = tags['sample']
        targetIntervals = B_RTC.get_nodes_by(sample=sample_name)[0].output_paths['targetIntervals']
        B_IR.add_node(name=sample_name,
                      pcmd = commands.IndelRealigner(input_bams=input_bam,
                                                    targetIntervals=targetIntervals,
                                                    output_bam='{output_dir}/{outputs[bam]}',
                                                    ),
                      outputs = {'bam':'{0}.realigned.bam'.format(sample_name)},
                      tags = {'sample':sample_name},
                      mem_req=2000)
    WF.run_wait(B_IR)
    
### Base Quality Score Recalibration
     
B_BQSR = WF.add_batch("Base Quality Score Recalibration") #TODO test using SW
if not B_BQSR.successful:
    for tags,input_nodes in B_clean_sam.group_nodes_by('sample'):
        sample_name = tags['sample']
        input_bam = input_nodes[0].output_paths['bam']
        B_BQSR.add_node(name=sample_name,
                        pcmd = commands.BaseQualityScoreRecalibration(input_bam = input_bam,
                                                                      output_recal_report='{output_dir}/outputs[recal]'),
                        outputs = {'recal':'{0}.recal'.format(sample_name)},
                        tags = {'sample':sample_name},
                        mem_req=2000)
    WF.run_wait(B_BQSR)
    
B_PR = WF.add_batch("Apply BQSR") #TODO test using SW
if not B_PR.successful:
    for tags,input_nodes in B_clean_sam.group_nodes_by('sample'):
        sample_name = tags['sample']
        input_bam = input_nodes[0].output_paths['bam']
        B_PR.add_node(name=sample_name,
                      pcmd = commands.PrintReads(input_bam = input_bam,
                                                 output_recal_report='{output_dir}/outputs[recal]'),
                      outputs = {'bam':'{0}.recalibrated.bam'.format(sample_name)},
                      tags = {'sample':sample_name},
                      mem_req=2000)
    WF.run_wait(B_PR) 

### Genotype

contigs = [str(x) for x in range(1,23)+['X','Y']] #list of chroms: [1,2,3,..X,Y]

B_UG = WF.add_batch(name="Unified Genotyper")
if not B_UG.successful:
    for tags,input_nodes in B_clean_sam.group_nodes_by('sample'):
        input_bam = input_nodes[0].output_paths['bam']
        sample_name = tags['sample']
        for chrom in contigs:
            for glm in ['INDEL','SNP']:
                B_UG.add_node(name='{0} {1} chr{2}'.format(sample_name,glm,chrom),
                              pcmd = commands.UnifiedGenotyper(input_bams=input_bam,
                                                            output_bam='{output_dir}/{outputs[vcf]}',
                                                            glm=glm),
                              outputs = {'vcf':'raw.vcf'.format(sample_name)},
                              tags = {'sample':sample_name,'chr':chrom, 'glm':glm},
                              mem_req=2000)
    WF.run_wait(B_UG)

# Merge VCFS

B_CV1 = WF.add_batch(name="Combine Variants into Sample VCFs")
if not B_CV1.successful:
    for tags,nodes_by_chr in B_UG.group_nodes_by('glm','sample'):
        vcfs_by_chr = [ n.output_paths['vcf'] for n in nodes_by_chr ]
        B_CV1.add_node(name=sample.name,
                      pre_command=commands.CombineVariants(input_vcfs=vcfs_by_chr,
                                                           output_vcf="{output_dir}/{outputs[vcf]}",
                                                           genotypeMergeOptions='REQUIRE_UNIQUE',
                                                           tags=tags),
                      outputs={'vcf':'raw.vcf'})
    WF.run_wait(B_CV1)

B_CV2 = WF.add_batch(name="Combine Variants into MasterVCF")
if not B_CV2.successful:
    for tags,input_nodes in B_CV1.group_nodes_by('glm'):
        sample_vcfs = [ n.output_paths['vcf'] for n in input_nodes ]
        B_CV2.add_node(name=sample.name,
                      pre_command=commands.CombineVariants(input_vcfs=vcfs_by_chr,
                                                           output_vcf="{output_dir}/{outputs[vcf]}",
                                                           genotypeMergeOptions='PRIORITIZE',
                                                           tags=tags),
                      outputs={'vcf':'raw.vcf'})
    WF.run_wait(B_CV2)


### Variant Recalibration

B_VQR = WF.add_batch(name="Variant Quality Recalibration")
if not B_VQR.successful:
    for tags,input_nodes in B_CV2.group_nodes_by('glm'): #only two files at this point
        input_vcf = input_nodes[0].output_paths['vcf']
        B_VQR.add_node(name=sample.name,
                       pre_command=commands.VariantQualityRecalibration(input_vcf=input_vcf,
                                                                        inbreedingcoeff=False,
                                                                        output_recal="{output_dir}/{outputs[recal]}",
                                                                        output_tranches="{output_dir}/{outputs[tranches]}",
                                                                        output_rscript="{output_dir}/{outputs[rscript]}",
                                                                        mode=glm,
                                                                        exome_or_wgs='exome',
                                                                        haplotypeCaller_or_unifiedGenotyper='UnifiedGenotyper'),
                      outputs={'recal':'output.recal','tranches':'output.tranches','rscript':'plot.R'})
    WF.run_wait(B_VQR)

B_AR = WF.add_batch(name="Apply Recalibration")
if not B_AR.successful:
    for tags,input_nodes in B_VQR.group_nodes_by('glm'): #only two files at this point
        vqr_node = input_nodes[0]
        B_AR.add_node(name='SNP',
                      pre_command=commands.ApplyRecalibration(input_vcf=input_vcf,
                                                              input_recal=vqr_node.output_paths['recal'],
                                                              input_tranches=vqr_node.output_paths['tranches'],
                                                              output_recalibrated_vcf="{output_dir}/{outputs[vcf]}",
                                                              mode=glm,
                                                              haplotypeCaller_or_unifiedGenotyper='UnifiedGenotyper'),
                      outputs={'vcf':'recalibrated.vcf'})
    WF.run_wait(B_AR)
        
WF.finished()
