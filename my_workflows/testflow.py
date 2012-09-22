#Import Cosmos
import sys
import cosmos_session

#Begin Workflow
from Workflow.models import Workflow, Batch

workflow = Workflow.restart(name='Test_Workflow')
assert isinstance(workflow, Workflow)

batch_sleep = workflow.add_batch("Long_Running_Job")
batch_sleep.add_node(name='1', pcmd='sleep 30')
workflow.run_batch(batch_sleep)

batch_echo = workflow.add_batch("Echo")
batch_echo.add_node(name='1', pcmd='echo "hello" > {output_dir}/{outputs[out]}', outputs = {'out':'echo.out'}, reverse='yes')
batch_echo.add_node(name='2', pcmd='echo "world" > {output_dir}/{outputs[out]}', outputs = {'out':'echo.out'}, reverse='yes')
batch_echo.add_node(name='3', pcmd='echo "don\'t reverse me" > {output_dir}/{outputs[out]}', outputs = {'out':'echo.out'}, reverse='no')
workflow.run_wait(batch_echo)

batch_reverse = workflow.add_batch("Reverse")
for node in workflow.get_tagged_nodes(reverse="yes"):
    input_path = node.outputs_fullpaths['out']
    batch_reverse.add_node(name=node.name, pcmd='rev '+input_path+' > {output_dir}/{outputs[out]}', outputs = {'out':'rev.out'})
workflow.run_wait(batch_reverse)

batch_check_reattempt = workflow.add_batch("Check_Failure")
batch_check_reattempt.add_node(name=node.name, pcmd='i_wont_work', outputs = {})
workflow.run_wait(batch_check_reattempt)

workflow.finished()
