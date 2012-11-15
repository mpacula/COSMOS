import cosmos_session
import os,re
from Workflow.models import Workflow

wf = Workflow.start('gunzip GATK Bundle')
#in_dir = '/nas/erik/bundle/1.5/b37'
in_dir = '/groups/lpm/erik/gatk/bundle/b37'
b = wf.add_stage("gunzip")
for f in filter(lambda x: re.match("^.*\.gz$",x), os.listdir(in_dir)):
    b.add_task(f,pcmd="gunzip {0}".format(os.path.join(in_dir,f)))
wf.run_wait(b,terminate_on_fail=False)
wf.finished()