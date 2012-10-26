Advanced Workflows
==================

Tracking Outputs
________________

You should actually avoid using STDOUT to store the output of your jobs, and redirect it to a file.  Cosmos helps you keep track
of these output files using a dictionary you pass to :py:meth:`Batch.add_node()` using the output parameter.  Time to really dig into the add_node() api:

.. automethod:: Workflow.models.Batch.add_node
   :noindex:

Understanding the add_node parameters are vital.  The two most complicated parameters are *pcmd* and *outputs*.  Cosmos automatically creates and output directory
tree for each Workflow, Batch, and Node.  You can access this system output_path by using *'{output_dir}'* in your pcmd.  Cosmos will also keep track of output directories for you.  Use the
*outputs* parameter to pass in a dictionary of output files.  You can then access the outputs directionary later by using :py:data:`Workflow.models.Node.outputs`, or access the full path to those output files
using :py:data:`Workflow.models.Node.output_paths`


.. code-block:: python
   :linenos:

   import cosmos.session
   from Workflow.models import Workflow
   import os
   
   WF = Workflow.start('My Advanced Workflow')
   
   Echo = WF.add_batch('Wcho')
   Echo.add_node(name='echo1', pcmd='echo "Hello World" > {output_dir}/{outputs[txt]}',outputs={'txt':'echo_out.txt'})
   Echo.add_node(name='echo2', pcmd='echo "Hello World2" > {output_dir}/{outputs[txt]}',outputs={'txt':'echo_out.txt'})
   Echo.add_node(name='echo3', pcmd='echo "Hello World3" > {output_dir}/{outputs[txt]}',outputs={'txt':'echo_out.txt'})
   WF.run_and_wait(Echo)
   
   Rev = WF.add_batch('Rev')
   for n in Echo.nodes:
       Rev.add_node(name='rev %s' % n.name,
                    pcmd='rev {1} > {{output_dir}}/{{outputs[rev_txt]}}'.format(n.output_paths['txt']), # Double braces escapes the .format() call
                    outputs={'rev_txt','reversed.txt'}) 
   WF.run_and_wait(Rev)
   
   WF.finished()
   
Steps
_____

See ``my_workflows/gatk*`` and ``my_workflows/examples/`` for examples on using the step addon for creating workflows.