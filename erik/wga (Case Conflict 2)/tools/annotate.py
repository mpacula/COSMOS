from cosmos.contrib.ezflow.tool import Tool
import os

class ANNOVAR(Tool):
    __verbose__ = "Annovar"
    inputs = ['vcf']
    outputs = ['tsv']
    one_parent = True

    def cmd(self,i,t,s,p):
        return 'annovar {i[vcf]} {t[database]}'

class PROCESS_ANNOVAR(Tool):
    __verbose__ = "Process Annovar"
    inputs = ['tsv']
    outputs = ['tsv']
    one_parent = True

    #    @opoi
    def cmd(self,i,t,s,p):
        return 'genomekey {i[tsv]}'

class MERGE_ANNOTATIONS(Tool):
    __verbose__ = "Merge Annotations"
    inputs = ['tsv']
    outputs = ['tsv']
    one_parent = True

    def cmd(self,i,t,s,p):
        return 'genomekey merge {0}'.format(','.join(map(lambda x:str(x),i['tsv'])))

class SQL_DUMP(Tool):
    __verbose__ = "SQL Dump"
    inputs = ['tsv']
    outputs = ['sql']
    one_parent = True

    def cmd(self,i,t,s,p):
        return 'sql dump {i[tsv]}'

class ANALYSIS(Tool):
    __verbose__ = "Filtration And Analysis"
    inputs = ['sql']
    outputs = ['analysis']
    one_parent = True

    def cmd(self,i,t,s,p):
        return 'analyze {i[sql]}'
