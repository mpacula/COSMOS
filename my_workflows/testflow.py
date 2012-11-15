#Import Cosmos
import sys
import cosmos.session

#Begin Workflow
from cosmos.Workflow.models import Workflow
from datetime import time

workflow = Workflow.start(name='Test_Workflow',restart=True)
assert isinstance(workflow, Workflow)

stage_sleep = workflow.add_stage("Independent Job")
n = stage_sleep.add_task(name='1', pcmd='sleep 75', mem_req=10,time_limit=time(0,1))

workflow.run_stage(stage_sleep)

stage_echo = workflow.add_stage("Echo")
stage_echo.add_task(name='1', pcmd='echo "hello" > {output_dir}/{outputs[out]}', outputs = {'out':'echo.out'}, tags={'reverse':'no'})
stage_echo.add_task(name='2', pcmd='echo "world" > {output_dir}/{outputs[out]}', outputs = {'out':'echo.out'}, tags={'reverse':'yes'})
stage_echo.add_task(name='3', pcmd='echo "don\'t reverse me" > {output_dir}/{outputs[out]}', outputs = {'out':'echo.out'},  tags={'reverse':'yes'})
workflow.run_wait(stage_echo)

stage_reverse = workflow.add_stage("Reverse the Echo")
for task in stage_echo.get_tasks_by({'reverse':"yes"}):
    input_path = task.output_paths['out']
    stage_reverse.add_task(name=task.name, pcmd='rev %s > {output_dir}/{outputs[out]}'%input_path, outputs = {'out':'rev.out'})
workflow.run_wait(stage_reverse)

stage_check_reattempt = workflow.add_stage("Fail")
stage_check_reattempt.add_task(name=task.name, pcmd='i_wont_work', outputs = {})
workflow.run_wait(stage_check_reattempt)

workflow.finished()
