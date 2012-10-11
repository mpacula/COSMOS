from Cosmos.addons.step import Step

class Paste(Step):
    outputs = {'txt':'out.txt'}
    
    def one2one_cmd(self,input_node):
        return r"""
            paste {input} {input} > {{output_dir}}/{{outputs[txt]}}
        """, {'input':input_node.output_paths['txt'],}
    
class Cat(Step):
    outputs = {'txt':'cat.txt'}
    
    def one2many_cmd(self,input_node):
        for i in range(1,4):
            yield (r"""
                cat {input_node.output_paths[txt]} > {{output_dir}}/{{outputs[txt]}}
            """,
            {},
            {'count':i})
    
    def many2one_cmd(self,input_nodes):
        inputs = ' '.join([ n.output_paths['txt'] for n in input_nodes])
        return (r"""
             cat {inputs} > {{output_dir}}/{{outputs[txt]}}
         """,
        {'inputs':inputs})
         
    
    