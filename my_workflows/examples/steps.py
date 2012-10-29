from cosmos.contrib.step import Step

class Echo(Step):
    outputs = {'txt':'out.txt'}
    
    def many2many_cmd(self,input_batch=None,strings=[]):
        """
        :param strings: the items to echo, one item per node
        """
        for text in strings: 
            yield { 
                'pcmd' : r"""
                            echo "{input}" > {{output_dir}}/{{outputs[txt]}}
                        """, 
                'pcmd_dict': {'input':text},
                'new_tags': {}
            }
        
class Paste(Step):
    outputs = {'txt':'out.txt'}
    
    def one2one_cmd(self,input_node):
        """
        Paste the input file with itself
        """
        return {
                'pcmd': r"""
                            paste {input} {input} > {{output_dir}}/{{outputs[txt]}}
                        """,
                'pcmd_dict': {'input':input_node.output_paths['txt']}
                }
    
class Cat(Step):
    outputs = {'txt':'cat.txt'}
    
    def one2many_cmd(self,input_node,copies=2):
        """
        Cats a file 4 times
        """
        for i in range(1,copies+1):
            yield {
                   'pcmd' : r"""
                        cat {input_node.output_paths[txt]} > {{output_dir}}/{{outputs[txt]}}
                    """,
                    'add_tags': {'count':i}
                    }
    
    def many2one_cmd(self,input_nodes,tags):
        """
        Cats all input_node txt outputs into one file
        """
        inputs = ' '.join([ n.output_paths['txt'] for n in input_nodes])
        return {
                'pcmd': r"""
                         cat {inputs} > {{output_dir}}/{{outputs[txt]}}
                     """,
                'pcmd_dict':{'inputs':inputs}
                }
         
    
    