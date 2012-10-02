#Import Cosmos
import sys,os
import cosmos_session
from Workflow.models import Workflow
from Cosmos.helpers import parse_cmd

input_files_path = '/home2/himanshu/rna-seq-fusion/input'

workflow = Workflow.restart(name='Mouse_RNA_Seq_fusion_3')


sequences = [
          ('s_1_1_sequence.txt ','s_1_2_sequence.txt'),
          ('s_2_1_sequence.txt ','s_2_2_sequence.txt'),
          ('s_3_1_sequence.txt','s_3_2_sequence.txt'),
          ('s_4_1_sequence.txt','s_4_2_sequence.txt'),
          ('s_5_1_sequence.txt','s_5_2_sequence.txt'),
          ('s_6_1_sequence.txt','s_6_2_sequence.txt'),
          ('s_7_1_sequence.txt','s_7_2_sequence.txt')
          ]


fusion_b1_batch = workflow.add_batch("tophat_fusion_bowtie1")
#fusion_b2_batch = workflow.add_batch("fusions_bowtie2")
#for bowtie_version in ['bowtie1','bowtie2']:
for i, seqs in enumerate(sequences):
    seq1 = os.path.join(input_files_path,seqs[0])
    seq2 = os.path.join(input_files_path,seqs[1])
    cmd = r"""
    /home2/himanshu/bin/tophat2 \
    --fusion-min-dist 100000 \
    --fusion-ignore-chromosomes chrM \
    -o {output_dir} \
    --fusion-search \
    --keep-fasta-order \
    --bowtie1 \
    --no-coverage-search \
    --mate-inner-dist 11 \
    --mate-std-dev 57 \
    /home2/himanshu/mm9_bowtie_index/mm9 \
    {seq1} {seq2}
    """
    cmd = parse_cmd(cmd,seq1=seq1,seq2=seq2,output_dir='{output_dir}')
    fusion_b1_batch.add_node(name="seq{0}".format(i+1),
                             pcmd = cmd,
                             outputs = {})
        
        
workflow.run_batch(fusion_b1_batch)
#workflow.run_batch(fusion_b2_batch)
workflow.wait()
