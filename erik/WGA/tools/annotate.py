from cosmos.contrib.ezflow.tool import Tool
import os

class ANNOVAR(Tool):
    name = "Annovar"
    inputs = ['vcf']
    outputs = ['tsv']
    

    def cmd(self,i,t,s,p):
        return 'annovar {i[vcf]} {t[database]}'

class PROCESS_ANNOVAR(Tool):
    name = "Process Annovar"
    inputs = ['tsv']
    outputs = ['tsv']
    
    def cmd(self,i,t,s,p):
        return 'genomekey {i[tsv]}'

class MERGE_ANNOTATIONS(Tool):
    name = "Merge Annotations"
    inputs = ['tsv']
    outputs = ['tsv']
    
    def cmd(self,i,t,s,p):
        return 'genomekey merge {0}'.format(','.join(map(lambda x:str(x),i['tsv'])))

class SQL_DUMP(Tool):
    name = "SQL Dump"
    inputs = ['tsv']
    outputs = ['sql']
    
    def cmd(self,i,t,s,p):
        return 'sql dump {i[tsv]}'

class ANALYSIS(Tool):
    name = "Filtration And Analysis"
    inputs = ['sql']
    outputs = ['analysis']
    
    def cmd(self,i,t,s,p):
        return 'analyze {i[sql]}'
