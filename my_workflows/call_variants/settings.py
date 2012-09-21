import os

###User Defined Settings
#This pipeline's settings
GATK_path = '/home2/erik/2gatk/GenomeAnalysisTK-2.1-8-g5efb575'
Picard_path = '/home2/erik/picard-tools-1.74'
resource_bundle_path = '/home2/erik/gatk/bundle/b37'
queue_output_dir = '/vol3/cai_testgluster'
tmp_dir='/mnt/tmp'


### auto generated
GATK_cmd = 'java -Xmx5g -Djava.io.tmpdir={tmp_dir} -jar {gatk_jar}'.format(gatk_jar = os.path.join(GATK_path,'GenomeAnalysisTK.jar'),tmp_dir=tmp_dir)
reference_fasta_path = os.path.join(resource_bundle_path,'human_g1k_v37.fasta')
dbsnp_path = os.path.join(resource_bundle_path,'dbsnp_135.b37.vcf')
hapmap_path = os.path.join(resource_bundle_path,'hapmap_3.3.b37.sites.vcf')
omni_path = os.path.join(resource_bundle_path,'1000G_omni2.5.b37.sites.vcf')
mills_path = os.path.join(resource_bundle_path,'Mills_and_1000G_gold_standard.indels.b37.sites.vcf')
#Setup Cosmos

   
from Cosmos import cosmos_session #the magic line to import Cosmos and Django compatibility