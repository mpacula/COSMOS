#Import Cosmos
import sys,os,re
import cosmos_session
from Workflow.models import Workflow, Batch

import subprocess
WF = Workflow.resume(name='GPP_48Exomes_GATK',dry_run=False)
assert isinstance(WF, Workflow)

input_dir = '/nas/erik/48exomes'

class Pool:
    def __init__(self,name,path,samples):
        self.name = name
        self.path = path
        self.samples = samples
        
class Sample:   
    def __init__(self,name,path):
        self.name = name
        self.path = path
        
pools = []

#get input files   

for pool_dir in os.listdir(input_dir):
    pool = Pool(name=pool_dir,path=os.path.join(input_dir,pool_dir),samples=[])
    pools.append(pool)
    print pool.name
    for sample_dir in filter(lambda x: re.search('Sample',x),os.listdir(pool.path)):
        sample = Sample(name=sample_dir,path=os.path.join(pool.path,sample_dir))
        pool.samples.append(sample)

B_gunzip = WF.add_batch("gunzip")
for pool in pools:
    cmd = 'find {0} -name *.gz'.format(pool.path)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    gzs = p.communicate()[0]
    for gz in gzs.split("\n"):
        if gz != '':
            name = os.path.basename(gz)
            B_gunzip.add_node(name=name,pre_command='gunzip -v {0}'.format(gz)) 
    #B_gunzip.add_node(name=pool.name,pre_command='gunzip -r -v {0}'.format(pool.path))    
WF.run_batch(B_gunzip)
WF.wait(B_gunzip)
    
WF.finished()
