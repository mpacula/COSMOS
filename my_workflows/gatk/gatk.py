import cosmos_session
from Workflow.models import Workflow, Batch
import subprocess
import commands
import os
from Cosmos.helpers import parse_command_string
from datetime import time
#from exomes48 import samples,input_dir

WF = Workflow.resume(name='GPP_48Exomes_GATK',dry_run=False)
assert isinstance(WF, Workflow)

#Gunzip fastqs
B_gunzip = WF.add_batch("gunzip")
if not B_gunzip.successful:
    cmd = 'find {0} -name *.gz'.format(input_dir)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    gzs = p.communicate()[0]
    for gz in gzs.split("\n"):
        if gz != '':
            name = os.path.basename(gz)
            B_gunzip.add_node(name=name,pcmd='gunzip -v {0}'.format(gz))
    WF.run_wait(B_gunzip)

B_bwa_aln = WF.add_batch("BWA Align")
if not B_bwa_aln.successful:
    for sample in samples:
        for fqp in sample.fastq_pairs:
            fqp.r1_sai_node = B_bwa_aln.add_node(name = fqp.r1,
                               pcmd = commands.bwa_aln(fastq=fqp.r1_path,output_sai='{output_dir}/{outputs[sai]}'),
                               outputs = {'sai':'{0}.sai'.format(fqp.r1)},
                               mem_req=3000,
                               time_limit=time(1))
            fqp.r2_sai_node = B_bwa_aln.add_node(name = fqp.r2,
                               pcmd = commands.bwa_aln(fastq=fqp.r2_path,output_sai='{output_dir}/{outputs[sai]}'),
                               outputs = {'sai':'{0}.sai'.format(fqp.r1)},
                               mem_req=3000,
                               time_limit=time(1)) #TODO add tags
    WF.run_wait(B_bwa_aln)

B_bwa_sampe = WF.add_batch("BWA Sampe",hard_reset=False)
if not B_bwa_aln.successful:
    for sample in samples:
        for fqp in sample.fastq_pairs:
            name = '{s.name} {fqp.lane} {fqp.readGroupNumber}'.format(s=sample,fqp=fqp) #next time use this for name and output
            fqp.sam_node = B_bwa_sampe.add_node(name = re.sub('_R1','',fqp.r1),
                               pcmd = commands.bwa_sampe(r1_sai=fqp.r1_sai_node.output_paths['sai'],
                                                         r2_sai=fqp.r2_sai_node.output_paths['sai'],
                                                         r1_fq=fqp.r1_path,
                                                         r2_fq=fqp.r2_path,
                                                         ID='%s.L%s' % (sample.flowcell,fqp.lane),
                                                         LIBRARY='LIB-%s' % sample.name,
                                                         SAMPLE_NAME=sample.name,
                                                         PLATFORM='ILLUMINA',
                                                         output_sam='{output_dir}/{outputs[sam]}'),
                               outputs = {'sam':'{0}.sam'.format(name)},
                               tags = {
                                   'sample':sample.name,
                                   'lane': fqp.lane,
                                   'readGroupNumber': fqp.readGroupNumber
                               },
                               mem_req=3000,
                               time_limit=time(2,45))
    WF.run_wait(B_bwa_sampe)

B_clean_sam = WF.add_batch("Clean Bams",hard_reset=False)
if not B_clean_sam.successful:
    for n in B_bwa_sampe.nodes:
        name = '{tags[sample]} L{tags[lane]} RGN{tags[readGroupNumber]}'.format(tags=n.tags)
        B_clean_sam.add_node(name=name,
                          pcmd = commands.CleanSam(input=n.output_paths['sam'],
                                                   output='{output_dir}/{outputs[bam]}'),
                          outputs = {'bam':'cleaned.bam'},
                          tags = n.tags,
                          mem_req=500,
                          time_limit=time(1))
WF.run_wait(B_clean_sam)

B_merge1 = WF.add_batch("Merge Bams by Sample",hard_reset=True)
if not B_merge1.successful:
    for tags,input_nodes in B_bwa_sampe.group_nodes_by('sample'):
        sample_name = tags['sample']
        sample_sams = [ n.output_paths['sam'] for n in input_nodes ]
        B_merge1.add_node(name=sample_name,
                          pcmd = commands.MergeSamFiles(input_bams=sample_sams,
                                                        output_bam='{output_dir}/{outputs[bam]}',
                                                        assume_sorted=False),
                          outputs = {'bam':'{0}.bam'.format(sample_name)},
                          tags = {'sample':sample_name},
                          mem_req=1000,
                          time_limit=time(1))
    WF.run_wait(B_merge1)
                                            
        
WF.finished()