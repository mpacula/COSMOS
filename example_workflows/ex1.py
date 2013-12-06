from cosmos.models import Workflow
from cosmos import session
from cosmos.flow import ToolGraph, one2many
from tools import ECHO, CAT
import os
opj = os.path.join

session.jobinfo_output_dir = lambda jobAttempt: opj(jobAttempt.workflow.output_dir, 'log',
                                                jobAttempt.task.stage.name.replace(' ', '_'),
                                                jobAttempt.task.tags_as_query_string().replace('&', '__').replace('=', '_'))
session.task_output_dir = lambda task: task.workflow.output_dir
session.stage_output_dir = lambda stage: stage.workflow.output_dir


wf = Workflow.start('Example 1', restart=True, prompt_confirm=False)

g = ToolGraph()
echo = g.source([ECHO(tags={'word': 'hello'}), ECHO(tags={'word': 'world'})])
cat  = g.stage(CAT, parents=[echo], rel=one2many([('n', [1, 2])]))
g.resolve()
g.as_image('stage', '/tmp/graph1.svg')
g.as_image('tool', '/tmp/graph2.svg')
g.add_run(wf)
