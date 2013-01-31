from cosmos.contrib.ezflow.tool import Tool

class ECHO(Tool):
    outputs = ['text']
    
    def cmd (self,i,s,p):
        return 'echo {p[word]} > $OUT.text'
    
class CAT(Tool):
    inputs = ['text']
    outputs = ['txt']
    
    def cmd(self,i,s,p):
        return 'cat {input} > $OUT.txt', {
                'input':' '.join(map(lambda x: str(x),i['text']))
                }
    
class PASTE(Tool):
    inputs = ['txt']
    outputs = ['txt']
    
    def cmd(self,i,s,p):
        return 'paste {input} > $OUT.txt', {
                'input':' '.join(map(lambda x: str(x),i['txt']))
                }
    
class WC(Tool):
    inputs = ['txt']
    outputs = ['txt']

    default_para = { 'args': '' }
    
    def cmd(self,i,s,p):
        return 'wc {input} > $OUT.txt', {
                'input':' '.join(map(lambda x: str(x),i['txt']))
                }