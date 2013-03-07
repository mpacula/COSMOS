import os
from cosmos import session
from cosmos.config import settings


if settings['server_name'] == 'gpp':
    resource_bundle_path = '/nas/erik/bundle/1.5/b37'
    tools_dir = '/home/ch158749/tools'
    wga_settings = {
        'GATK_path' : os.path.join(tools_dir,'GenomeAnalysisTKLite-2.1-8-gbb7f038/GenomeAnalysisTKLite.jar'),
        'Picard_dir' : os.path.join(tools_dir,'picard-tools-1.77'),
        'bwa_path' : os.path.join(tools_dir,'bwa-0.6.2/bwa'),
        'bwa_reference_fasta_path' : '/nas/erik/bwa_reference/human_g1k_v37.fasta',
        'samtools_path':os.path.join(tools_dir,'samtools-0.1.18/samtools'),
        'get_drmaa_native_specification':session.default_get_drmaa_native_specification
        }

elif settings['server_name']  == 'orchestra':
    def get_drmaa_native_specification(jobAttempt):
        task = jobAttempt.task
        DRM = settings['DRM']

        cpu_req = task.cpu_requirement
        mem_req = task.memory_requirement
        time_req = task.time_requirement
        queue = task.workflow.default_queue

        if time_req < 10:
            queue = 'mini'
        if time_req < 12*60:
            queue = 'short'
        else:
            queue = 'long'

        if DRM == 'LSF':
            s = '-R "rusage[mem={0}] span[hosts=1]" -n {1}'.format(mem_req,cpu_req)
            if time_req:
                s += ' -W 0:{0}'.format(time_req)
            if queue:
                s += ' -q {0}'.format(queue)
            return s
        else:
            raise Exception('DRM not supported')

    resource_bundle_path = '/groups/lpm/erik/WGA/bundle/2.2/b37'
    tools_dir = '/groups/lpm/erik/WGA/tools'
    wga_settings = {
        'GATK_path' :os.path.join(tools_dir,'GenomeAnalysisTKLite-2.2-16-g2cc9ef8/GenomeAnalysisTKLite.jar'),
        'Picard_dir' : os.path.join(tools_dir,'picard-tools-1.78'),
        'bwa_path' : os.path.join(tools_dir,'bwa-0.6.2/bwa'),
        'bwa_reference_fasta_path' : '/groups/lpm/erik/WGA/bwa_reference/human_g1k_v37.fasta',
        'samtools_path':os.path.join(tools_dir,'samtools-0.1.18/samtools'),
        'get_drmaa_native_specification': get_drmaa_native_specification
        }

elif settings['server_name']  == 'bioseq':
    resource_bundle_path = '/cosmos/WGA/bundle/2.2/b37/'
    tools_dir = '/cosmos/WGA/tools'
    wga_settings = {
        'GATK_path' :os.path.join(tools_dir,'GenomeAnalysisTKLite-2.2-16-g2cc9ef8/GenomeAnalysisTKLite.jar'),
        'Picard_dir' : os.path.join(tools_dir,'picard-tools-1.81'),
        'bwa_path' : os.path.join(tools_dir,'bwa-0.6.2/bwa'),
        'bwa_reference_fasta_path' : '/cosmos/WGA/bwa_reference/human_g1k_v37.fasta',
        'samtools_path':os.path.join(tools_dir,'samtools-0.1.18/samtools')
        }
else:
    resource_bundle_path = '/gluster/gv0/WGA/bundle/2.2/b37/'
    tools_dir = '/gluster/gv0/WGA/tools'
    wga_settings = {
        'GATK_path' :os.path.join(tools_dir,'GenomeAnalysisTKLite-2.2-16-g2cc9ef8/GenomeAnalysisTKLite.jar'),
        'Picard_dir' : os.path.join(tools_dir,'picard-tools-1.84'),
        'bwa_path' : os.path.join(tools_dir,'bwa-0.6.2/bwa'),
        'bwa_reference_fasta_path' : '/gluster/gv0/WGA/bwa_reference/human_g1k_v37.fasta',
        'samtools_path':os.path.join(tools_dir,'samtools-0.1.18/samtools')
    }

wga_settings.update({
    'resource_bundle_path' : resource_bundle_path,
    'tmp_dir' : settings['tmp_dir'],
    'reference_fasta_path' : os.path.join(resource_bundle_path,'human_g1k_v37.fasta'),
    'dbsnp_path' : os.path.join(resource_bundle_path,'dbsnp_137.b37.vcf'),
    'hapmap_path' : os.path.join(resource_bundle_path,'hapmap_3.3.b37.vcf'),
    'omni_path' : os.path.join(resource_bundle_path,'1000G_omni2.5.b37.vcf'),
    'mills_path' : os.path.join(resource_bundle_path,'Mills_and_1000G_gold_standard.indels.b37.vcf'),
    'indels_1000g_phase1_path' : os.path.join(resource_bundle_path,'1000G_phase1.indels.b37.vcf')
})
