import cosmos_session
from Workflow.models import Workflow
import steps

WF = Workflow.start('Example1',restart=True)

batch_HW = WF.add_batch("Hello World")
batch_HW.add_node(name='hello',
                  pcmd = 'echo hello > {output_dir}/{outputs[txt]}',
                  outputs={'txt':'file.txt'},
                  tags={'word':'hello'})
batch_HW.add_node(name='world',
                  pcmd = 'echo world > {output_dir}/{outputs[txt]}',
                  outputs={'txt':'file.txt'},
                  tags={'word':'world'})
WF.run_wait(batch_HW)

steps.workflow = WF
paste = steps.Paste("Paste").one2one(input_batch=batch_HW)
cat = steps.Cat("Cat o2m").one2many(paste)
cat2 = steps.Cat("Cat m2o").many2one(cat,['word'])