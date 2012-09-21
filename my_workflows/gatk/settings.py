import os

###User Defined Settings
#This pipeline's settings
#GATK_path = '/home2/erik/2gatk/GenomeAnalysisTK-2.1-8-g5efb575'
GATK_path = '/home/ch158749/tools/GenomeAnalysisTKLite-2.1-8-gbb7f038'
Picard_path = '/home2/ch158749/picard-tools-1.74'
bwa_path = '/home/ch158749/tools/bwa-0.6.2/bwa'
#resource_bundle_path = '/home2/erik/gatk/bundle/b37'
resource_bundle_path = '/nas/gatk_bundle1.5'
queue_output_dir = '/vol3/cai_testgluster'
tmp_dir='/mnt/tmp'


### auto generated
reference_fasta_path = os.path.join(resource_bundle_path,'human_g1k_v37.fasta')
dbsnp_path = os.path.join(resource_bundle_path,'dbsnp_135.b37.vcf')
hapmap_path = os.path.join(resource_bundle_path,'hapmap_3.3.b37.sites.vcf')
omni_path = os.path.join(resource_bundle_path,'1000G_omni2.5.b37.sites.vcf')
mills_path = os.path.join(resource_bundle_path,'Mills_and_1000G_gold_standard.indels.b37.sites.vcf')
#Setup Cosmos

def get_Picard_cmd(jar,memory="5g"):
    return 'java -Xmx5g -jar {picard_jar}'.format(picard_jar = os.path.join(Picard_path,jar))
def get_Gatk_cmd(jar,memory="5g"):
    return 'java -Xmx5g -Djava.io.tmpdir={tmp_dir} -jar {gatk_jar}'.format(gatk_jar = os.path.join(GATK_path,'GenomeAnalysisTK.jar'),tmp_dir=tmp_dir)
