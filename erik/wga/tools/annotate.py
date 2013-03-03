from cosmos.contrib.ezflow.tool import Tool

class AnnovarExt_DownDB(Tool):
    time_req = 10
    name = "Download Annotation Database"

    def cmd(self,i,s,p):
        return 'annovarext downdb {p[build]} {p[dbname]}'

class AnnovarExt_Anno(Tool):
    name = "Annovar"
    inputs = ['tsv']
    outputs = ['dir']
    forward_input=True
    time_req = 120

    def cmd(self,i,s,p):
        return 'annovarext anno {p[build]} {p[dbname]} {i[tsv][0]} $OUT.dir'

class AnnovarExt_vcf2anno(Tool):
    name = "Annovar"
    inputs = ['vcf']
    outputs = ['anno']
    forward_input=True
    time_req = 10

    def cmd(self,i,s,p):
        return 'annovarext vcf2anno {i[vcf][0]} > $OUT.dir'


class AnnovarExt_Merge(Tool):
    name = "Merge Annotations"
    inputs = ['tsv','dir']
    outputs = ['dir']
    mem_req = 10*1024
    time_req = 120
    
    def cmd(self,i,s,p):
        return ('annovarext merge {i[tsv][0]} $OUT.dir {annotated_dir_output}',
                { 'annotated_dir_output' : ' '.join(map(str,i['dir'])) }
        )

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
