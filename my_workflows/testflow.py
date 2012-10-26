#Import Cosmos
import sys
import cosmos.session

#Begin Workflow
from cosmos.Workflow.models import Workflow
from datetime import time

workflow = Workflow.start(name='Test_Workflow',restart=True)
assert isinstance(workflow, Workflow)

batch_sleep = workflow.add_batch("Independent Job")
n = batch_sleep.add_node(name='1', pcmd='sleep 75', mem_req=10,time_limit=time(0,1))

workflow.run_batch(batch_sleep)

batch_echo = workflow.add_batch("Echo")
batch_echo.add_node(name='1', pcmd='echo "hello" > {output_dir}/{outputs[out]}', outputs = {'out':'echo.out'}, tags={'reverse':'no'})
batch_echo.add_node(name='2', pcmd='echo "world" > {output_dir}/{outputs[out]}', outputs = {'out':'echo.out'}, tags={'reverse':'yes'})
batch_echo.add_node(name='3', pcmd='echo "don\'t reverse me" > {output_dir}/{outputs[out]}', outputs = {'out':'echo.out'},  tags={'reverse':'yes'})
workflow.run_wait(batch_echo)

batch_reverse = workflow.add_batch("Reverse the Echo")
for node in batch_echo.get_nodes_by({'reverse':"yes"}):
    input_path = node.output_paths['out']
    batch_reverse.add_node(name=node.name, pcmd='rev %s > {output_dir}/{outputs[out]}'%input_path, outputs = {'out':'rev.out'})
workflow.run_wait(batch_reverse)

batch_check_reattempt = workflow.add_batch("Fail")
batch_check_reattempt.add_node(name=node.name, pcmd='i_wont_work', outputs = {})
workflow.run_wait(batch_check_reattempt)

workflow.finished()
