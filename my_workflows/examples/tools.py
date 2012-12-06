from cosmos.contrib.ezflow.tool import Tool

class ECHO(Tool):
    outputs = ['text']
    
    def cmd (self,i,t,s,p):
        return 'echo {t[word]} > $OUT.text'
    
class CAT(Tool):
    inputs = ['text']
    outputs = ['txt']
    
    def cmd(self,i,t,s,p):
        return 'cat {0} > $OUT.txt'.format(' '.join(i['text']))
    
class PASTE(Tool):
    inputs = ['txt']
    outputs = ['txt']
    
    def cmd(self,i,t,s,p):
        return 'paste {0} > $OUT.txt'.format(' '.join(i['txt']))
    
class WC(Tool):
    inputs = ['txt']
    outputs = ['txt']
    
    def cmd(self,i,t,s,p):
        return 'wc{{p[args]}} {0} > $OUT.txt'.format(' '.join(i['txt']))