import os
import re
from cosmos.contrib.ezflow.tool import INPUT
from cosmos.Workflow.models import TaskFile
#deprecated
def get_inputs(dir):
    fastqs = os.listdir(dir)
    inputs = []
    for fq in fastqs:
        """
        tags: lane, chunk, library, sample, platform, flowcell, pair
        """
        tags = re.search("(?P<flowcell>.+)\.(?P<lane>\d+)_(?P<pair>\d+)\.fastq",fq).groupdict()
        tags['platform'] = 'ILLUMINA'
        tags['sample'] = os.path.basename(dir)
        tags['chunk'] = 1
        tags['library'] = 'LIB-'+tags['sample']
        i = INPUT(tags=tags,taskfile=TaskFile(path=os.path.join(dir,fq),fmt='fastq.gz'))
        inputs.append(i)
    return inputs

