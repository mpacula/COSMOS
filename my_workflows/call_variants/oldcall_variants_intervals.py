import settings #magic line

import re
from argh import arg,ArghParser,plain_signature,command
import os
from Workflow.models import Workflow, Batch
import commands
from Cosmos.helpers import addExt
import re

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

#Reduce Bams
#batch = workflow.add_batch(name="ReduceBam")
#for sample in samples:
#    sample.nodes['Reduced Bams By Contig'] = []
#    for contig in contigs:
#        node = batch.add_node(name='{}_{}'.format(sample.name,contig),
#                              pre_command=commands.ReduceBam(interval=contig,input_bam=sample.input_bam, output_bam='{output_dir}/{outputs[output_file]}'),
#                              outputs={'output_file':addExt(sample.input_bam,'reduced')})
#        sample.nodes['Reduced Bams By Contig'].append(node)    
#workflow.run_batch(batch)
#workflow.wait_on_all_nodes()
#
##Merge Reduced Bams
#batch = workflow.add_batch(name="Merge_Reduced_Bams")
#for sample in samples:
#    sample.nodes['ReduceBam'] = None
#    input_bams = [ node.outputs_fullpaths['output_file'] for node in sample.nodes['Reduced Bams By Contig'] ]
#    pre_command = commands.MergeSamFiles(input_bams=input_bams,
#                                         output_bam='{output_dir}/{outputs[reduced_bam]}')
#    outputs = {'reduced_bam':addExt(sample.input_bam,'reduced')}
#    node = batch.add_node(name=sample.name, pre_command=pre_command, outputs=outputs)
#    sample.nodes['ReduceBam'] = node
#workflow.run_batch(batch)
#workflow.wait_on_all_nodes() 
   
   

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
    
UG_SNP_batch = workflow.add_batch(name="UnifiedGenotyper_SNP",hard_reset=True)
mapSampesByContig(batch=UG_SNP_batch,
           samples=samples,
           input_batch = None,
           output_command=commands.UnifiedGenotyper_SNP,
           output_ext='vcf',
           use_raw_input=True,
           test_with_one_contig=True)
workflow.run_batch(UG_SNP_batch)


workflow.wait_on_all_nodes()

#mapSamplesByContig(batch=UGbatch,
#                   samples=samples,
#                   input_name=commands.ReduceBam,
#                   input_output_name = 'reduced_bam',
#                   output_name='snps.vcf',
#                   output_command=commands.UnifiedGenotyper_SNP)

#mapSamplesByContig(batch=UGbatch, samples=samples, input_command=commands.ReduceBam,
#                   input_output_name = 'ReduceBam',
#                   output_name='indels.vcf', output_command=commands.UnifiedGenotyper_INDEL)

#VariantRecalibration

#print workflow.toString()
