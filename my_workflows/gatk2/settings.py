import os
from cosmos_session import cosmos_settings
###User Defined Settings
#This pipeline's settings
#GATK_path = '/home2/erik/2gatk/GenomeAnalysisTK-2.1-8-g5efb575'
GATK_path = '/home/ch158749/tools/GenomeAnalysisTKLite-2.1-8-gbb7f038'
Picard_path = '/home/ch158749/tools/picard-tools-1.77'
bwa_path = '/home/ch158749/tools/bwa-0.6.2/bwa'
#resource_bundle_path = '/home2/erik/gatk/bundle/b37'
resource_bundle_path = '/nas/erik/bundle/1.5/b37'
queue_output_dir = '/vol3/cai_testgluster'
tmp_dir=cosmos_settings.tmp_dir


### auto generated
GATK_cmd = 'java -Xmx{memory} -Djava.io.tmpdir={tmp_dir} -jar {gatk_jar}'.format(memory="5g",gatk_jar = os.path.join(GATK_path,'GenomeAnalysisTKLite.jar'),tmp_dir=tmp_dir)
 
reference_fasta_path = os.path.join(resource_bundle_path,'human_g1k_v37.fasta')
bwa_reference_fasta_path = os.path.join('/nas/erik/bwa_reference','human_g1k_v37.fasta')
dbsnp_path = os.path.join(resource_bundle_path,'dbsnp_135.b37.vcf')
hapmap_path = os.path.join(resource_bundle_path,'hapmap_3.3.b37.sites.vcf')
omni_path = os.path.join(resource_bundle_path,'1000G_omni2.5.b37.sites.vcf')
mills_path = os.path.join(resource_bundle_path,'Mills_and_1000G_gold_standard.indels.b37.vcf')
indels_1000g_phase1_path = os.path.join(resource_bundle_path,'1000G_phase1.indels.b37.vcf')
#Setup Cosmos

def get_Picard_cmd(jar,memory="5g"):
    return 'java -Xmx{memory} -Djava.io.tmpdir={tmp_dir} -jar {picard_jar}'.format(memory=memory,picard_jar = os.path.join(Picard_path,jar),tmp_dir=tmp_dir)

