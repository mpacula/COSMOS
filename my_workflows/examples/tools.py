from cosmos.contrib.ezflow.tool import Tool

class ECHO(Tool):
    outputs = ['text']
    
    def cmd (self,i,t,s,p):
        return 'echo {t[word]} > $OUT.text'
    
class CAT(Tool):
    inputs = ['text']
    outputs = ['txt']
    
    def cmd(self,i,t,s,p):
        return 'cat {input} > $OUT.txt', {
                'input':' '.join(i['text'])
                }
    
class PASTE(Tool):
    inputs = ['txt']
    outputs = ['txt']
    
    def cmd(self,i,t,s,p):
        return 'paste {input} > $OUT.txt', {
                'input':' '.join(i['text'])
                }
    
class WC(Tool):
    inputs = ['txt']
    outputs = ['txt']
    
    def cmd(self,i,t,s,p):
        return 'wc{p[args]} {0input} > $OUT.txt', {
                'input':' '.join(i['text'])
                }