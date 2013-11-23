from cosmos.models import Workflow
from cosmos.flow import ToolGraph, one2many
from tools import ECHO, CAT

wf = Workflow.start('Example 1', restart=True)
g = ToolGraph()
echo = g.source([ECHO(tags={'word': 'hello'}), ECHO(tags={'word': 'world'})])
cat  = g.stage(CAT, parents=[echo], rel=one2many([('n', [1, 2])]))
g.resolve()
g.as_image('stage', '/tmp/graph1.svg')
g.as_image('tool', '/tmp/graph2.svg')
g.add_run(wf)