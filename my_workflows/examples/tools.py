from cosmos.contrib.ezflow.tool import Tool

class ECHO(Tool):
    outputs = ['txt']
    
    def cmd (self,i,t,p):
        return 'echo {t[word]} > $OUT.txt'
    
class CAT(Tool):
    inputs = ['txt']
    outputs = ['txt']
    
    def cmd(self,i,t,p):
        return 'cat {0} > $OUT.txt'.format(' '.join(i['txt']))
    
class PASTE(Tool):
    inputs = ['txt']
    outputs = ['txt']
    
    def cmd(self,i,t,p):
        return 'paste {0} > $OUT.txt'.format(' '.join(i['txt']))