from cosmos.contrib.ezflow.tool import Tool
import os

class AnnovarExt_DownDB(Tool):
    name = "Download Annotation Database"

    def cmd(self,i,s,p):
        return 'annovarext downdb {p[build]} {p[dbname]}'

class AnnovarExt_Anno(Tool):
    name = "Annovar"
    inputs = ['vcf']
    outputs = ['tsv']
    

    def cmd(self,i,s,p):
        return 'annovarext anno hg19 {i[vcf]} {t[database]}'

class AnnovarExt_Merge(Tool):
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
