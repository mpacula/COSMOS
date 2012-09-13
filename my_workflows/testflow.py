#Cosmos Settings
import cosmos_settings #the magic line to import Cosmos and Django compatibility


import re
import os
from Workflow.models import Workflow, Batch

workflow = Workflow.restart(name='Test_Workflow')
assert isinstance(workflow, Workflow)

batch_a = workflow.add_batch("Echo")
assert isinstance(batch_a,Batch)
batch_a.add_node(name='1', pre_command='echo "hello" > {output_dir}/{outputs[out]}', outputs = {'out':'echo.out'})
batch_a.add_node(name='2', pre_command='echo "world" > {output_dir}/{outputs[out]}', outputs = {'out':'echo.out'})
workflow.run_batch(batch_a)
workflow.wait_on_all_nodes()

batch_reverse = workflow.add_batch("Reverse")
assert isinstance(batch_reverse,Batch)
for node in batch_a.nodes:
    input_path = node.outputs_fullpaths['out']
    batch_reverse.add_node(name=node.name, pre_command='rev '+input_path+' > {output_dir}/{outputs[out]}', outputs = {'out':'rev.out'})
workflow.run_batch(batch_reverse)
workflow.wait_on_all_nodes()


batch_check_reattempt = workflow.add_batch("Check_Reattempt")
batch_check_reattempt.add_node(name=node.name, pre_command='i_wont_work', outputs = {})
workflow.run_batch(batch_check_reattempt)
workflow.wait_on_all_nodes()

