import cosmos_session
from Workflow.models import Workflow
from Cosmos.addons import step
import steps

WF = Workflow.start('Example1',default_queue='high_priority',restart=True)
step.workflow = WF

echo = steps.Echo('Echo').many2many(input_batch=None,strings='Hello World')
paste = steps.Paste("Paste").one2one(input_batch=echo)
cat = steps.Cat("Cat o2m").one2many(paste)
cat2 = steps.Cat("Cat m2o").many2one(cat)

WF.finished()