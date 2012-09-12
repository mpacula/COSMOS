import settings #magic line

import re
import os
from Workflow.models import Workflow, Batch
import commands

contigs = [str(x) for x in range(1,23)+['X','Y']] #list of chroms: [1,2,3,..X,Y]

#workflow = Workflow.restart(name='MGH_BC_Call_Variants_EBS_intervals',root_output_dir='/2GATK/Cosmos_Out')
workflow = Workflow.resume(name='MGH_BC_Call_Variants_EBS_intervals')
assert isinstance(workflow, Workflow)

class Sample:
    def __init__(self,name,input_bam):
        self.name = name
        self.input_bam = input_bam
        self.nodes = {}
    
samples = [
           Sample('N12-141',os.path.join('/2GATK/cai2','MGH_BC.N12-141.clean.dedup.recal.bam')),
           Sample('N12-140',os.path.join('/2GATK/cai2','MGH_BC.N12-140.clean.dedup.recal.bam'))
           ]
   

def mapSampesByContig(batch, samples, input_batch, output_command, output_ext, use_raw_input=False, test_with_one_contig=False):
    """
    Currently output_command can only be run once
    """
    for sample in samples:
        sample.nodes[batch.name] = [] #add key
        if use_raw_input:
            input_bam = sample.input_bam
        else:
            input_bam = sample.nodes[input_batch.name]
        for contig in contigs:
            pre_command = output_command(interval=contig,
                                      input_bam=input_bam,
                                      output_bam='{output_dir}/{outputs[output_file]}')
            name = "{}_chr{}".format(sample.name,contig)
            outputs = {'output_file':'{}_chr{}_{}'.format(sample.name,contig,output_ext)}
            
            node = batch.add_node(name=name,
                                      pre_command=pre_command,
                                      outputs=outputs)
            sample.nodes[batch.name].append(node)
            if test_with_one_contig:
                break
    return batch
    
UG_SNP_batch = workflow.add_batch(name="UnifiedGenotyper_SNP",hard_reset=False)
mapSampesByContig(batch=UG_SNP_batch,
           samples=samples,
           input_batch = None,
           output_command=commands.UnifiedGenotyper_SNP,
           output_ext='vcf',
           use_raw_input=True,
           test_with_one_contig=False)
workflow.run_batch(UG_SNP_batch)

UG_SNP_batch = workflow.add_batch(name="UnifiedGenotyper_INDEL",hard_reset=False)
mapSampesByContig(batch=UG_SNP_batch,
           samples=samples,
           input_batch = None,
           output_command=commands.UnifiedGenotyper_INDEL,
           output_ext='vcf',
           use_raw_input=True,
           test_with_one_contig=False)
workflow.run_batch(UG_SNP_batch)
workflow.wait_on_all_nodes()

CV_sample_batch = workflow.add_batch(name="CombineVariants_into_Sample_VCFs",hard_reset=False)
for sample in samples:
    split_vcfs = [(sample.name,node.outputs_fullpaths['output_file']) for node in sample.nodes['UnifiedGenotyper_SNP']]
    split_vcfs = split_vcfs + [(sample.name,node.outputs_fullpaths['output_file']) for node in sample.nodes['UnifiedGenotyper_INDEL']]
    node = CV_sample_batch.add_node(name=sample.name,
                              pre_command=commands.CombineVariants(input_vcfs=split_vcfs, output_vcf="{output_dir}/{outputs[vcf]}"),
                              outputs={'vcf':'raw_variants.vcf'})
    sample.nodes['CombineVariants_into_Sample_VCFs'] = node
workflow.run_batch(CV_sample_batch)
workflow.wait_on_all_nodes()

CV_master_batch = workflow.add_batch(name="CombineVariants_into_MasterVCF",hard_reset=False)
for sample in samples:
    sample_vcfs = (sample.name,sample.nodes['CombineVariants_into_Sample_VCFs'])
    node = CV_master_batch.add_node(name='master.Svcf',
                              pre_command=commands.CombineVariants(input_vcfs=split_vcfs, output_vcf="{output_dir}/{outputs[vcf]}"),
                              outputs={'vcf':'raw_variants.vcf'})
    sample.nodes['CombineVariants_into_MasterVCF'] = node
workflow.run_batch(CV_master_batch)
workflow.wait_on_all_nodes()

VR_batch = workflow.add_batch(name="VariantRecalibration",hard_reset=False)
for sample in samples:
    sample.nodes['VariantRecalibration'] = {'SNP':None,'INDEL':None}
    input_vcf = sample.nodes['CombineVariants_into_MasterVCF'].outputs_fullpaths['vcf']
    node = VR_batch.add_node(name=sample.name,
                              pre_command=commands.VariantQualityRecalibration(input_vcf=input_vcf,
                                                                               inbreedingcoeff=False,
                                                                               recal="{output_dir}/{outputs[recal]}",
                                                                               tranches="{output_dir}/{outputs[tranches]}",
                                                                               rscript="{output_dir}/{outputs[rscript]}",
                                                                               mode='SNP',
                                                                               exome_or_wgs='exome',
                                                                               haplotypeCaller_or_unifiedGenotyper='UnifiedGenotyper'
                                                                               ),
                              outputs={'recal':'output.recal','tranches':'output.trances','rscript':'plot.R'})
    sample.nodes['VariantRecalibration']['SNP'] = node
    node = VR_batch.add_node(name=sample.name,
                             pre_command=commands.VariantQualityRecalibration(input_vcf=input_vcf,
                                                                               inbreedingcoeff=False,
                                                                               recal="{output_dir}/{outputs[recal]}",
                                                                               tranches="{output_dir}/{outputs[tranches]}",
                                                                               rscript="{output_dir}/{outputs[rscript]}",
                                                                               mode='INDEL',
                                                                               exome_or_wgs='exome',
                                                                               haplotypeCaller_or_unifiedGenotyper='UnifiedGenotyper'
                                                                               ),
                             outputs={'recal':'output.recal','tranches':'output.trances','rscript':'plot.R'})
    sample.nodes['VariantRecalibration']['INDEL'] = node
workflow.run_batch(VR_batch)
workflow.wait_on_all_nodes()

