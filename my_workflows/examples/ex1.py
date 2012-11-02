import cosmos.session
from cosmos.Workflow.models import Workflow
from cosmos.contrib import step
import steps

WF = Workflow.start('Example1',default_queue='',restart=True)
step.workflow = WF

echo = steps.Echo('Echo').none2many(strings=['Hello','World','!'])
wc = steps.WordCount('WordCount').one2one(flags='-m')
paste = steps.Paste("Paste").one2one(input_batches=[echo,wc])
cat = steps.Cat("Cat o2m").one2many(paste,copies=2)
cat2 = steps.Cat("Cat m2o",hard_reset=True).many2one(cat,group_by=[])

WF.finished()