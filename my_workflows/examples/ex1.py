import cosmos.session
from cosmos.Workflow.models import Workflow
from cosmos.contrib import step
import steps

WF = Workflow.start('Example1',default_queue='',restart=True)
step.workflow = WF

echo = steps.Echo('Echo').none2many(strings=['Hello World','Hello World'])
paste = steps.Paste("Paste").one2one(input_batch=echo)
cat = steps.Cat("Cat o2m").one2many(paste,copies=2)
cat2 = steps.Cat("Cat m2o",hard_reset=True).many2one(cat)

WF.finished()