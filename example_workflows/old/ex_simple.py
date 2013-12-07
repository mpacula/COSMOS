"""
A Simple Workflow
"""
from cosmos.models.workflow.models import Workflow

wf = Workflow.start('Simple')
stage = wf.add_stage('My Stage')
task = stage.add_task('echo "hello world"')
wf.run()
