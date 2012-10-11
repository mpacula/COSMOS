import cosmos_session
from Workflow.models import Workflow
import steps
from sample import Sample,Fastq
import os

##make samples dictionary
input_dir='/nas/erik/test_data'
samples=[]

#for pool_dir in os.listdir(input_dir):
#    for sample_dir in filter(lambda x: x!='.DS_Store',os.listdir(os.path.join(input_dir,pool_dir))):
#        samples.append(Sample.createFromPath(os.path.join(input_dir,pool_dir,sample_dir)))
for sample_dir in filter(lambda x: x!='.DS_Store',os.listdir(input_dir)):
    samples.append(Sample.createFromPath(os.path.join(input_dir,sample_dir)))


WF = Workflow.start(name='GPP 48Exomes GATK2',restart=False)
assert isinstance(WF, Workflow)

contigs = [str(x) for x in range(1,23)+['X','Y']] #list of chroms: [1,2,3,..X,Y]

### Alignment

B_bwa_aln = WF.add_batch("BWA Align")
if not B_bwa_aln.successful:
    for sample in samples:
        for fqp in sample.yield_fastq_pairs():
            for fq in fqp:
                B_bwa_aln.add_node(name = fq.filename,
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
    WF.run_wait(B_bwa_aln) 

steps.step.workflow = WF
bwa_sampe = steps.BWA_Sampe("BWA Sampe").many2one(input_batch=B_bwa_aln,group_by=['sample','lane','fq_partNumber'])
#bwa_sampe = steps.BWA_Sampe("BWA Sampe").many2one(input_batch=B_bwa_aln,group_by=['sample','lane','fq_partNumber'])
#bwa_sampe = steps.BWA_Sampe("BWA Sampe").many2one(input_batch=B_bwa_aln,group_by=['sample','lane','fq_partNumber'])
#bwa_sampe = steps.BWA_Sampe("BWA Sampe").many2one(input_batch=B_bwa_aln,group_by=['sample','lane','fq_partNumber'])
    
    
WF.finished(delete_unused_batches=True)
