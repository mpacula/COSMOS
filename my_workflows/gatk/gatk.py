#Import Cosmos
import sys
cosmos_path = '/home/ch158749/workspace/Cosmos'
if cosmos_path not in sys.path:
    sys.path.append(cosmos_path)
import cosmos_session

import os
from Workflow.models import Workflow, Batch
import commands

contigs = [str(x) for x in range(1,23)+['X','Y']] #list of chroms: [1,2,3,..X,Y]

workflow = Workflow.resume(name='GPP_48Exomes_GATK')
assert isinstance(workflow, Workflow)

#get input files   
os.listdir('/mnt/bch_gp/Public/LabCorp_run1')
/mnt/bch_gp/Public/LabCorp_run1/Project_BCH_pool1_r1

workflow.finished()
