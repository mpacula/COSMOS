from flow import create_dag_img, Apply, Reduce, Split, ReduceSplit, run
from gatk_tasks import *

input_data = [
         #Sample, fq_chunk, fq_pair, output_path
         ('A',1,1,'/data/A_1_1.fastq'),
         ('A',1,2,'/data/A_1_2.fastq'),
         ('A',2,1,'/data/A_2_1.fastq'),
         ('A',2,2,'/data/A_2_2.fastq'),
         ('B',1,1,'/data/B_1_1.fastq'),
         ('B',1,2,'/data/B_1_2.fastq'),
         ]
INPUT = [FASTQ(tags={'sample':x[0],'fq_chunk':x[1],'fq_pair':x[2]},outputs={'fastq':x[3]}) for x in input_data]


intervals = ('interval',[3,4])
glm = ('glm',['SNP','INDEL'])
dbs = ('database',['1000G','PolyPhen2','COSMIC','ENCODE'])

run(
    INPUT
    |Apply| ALN
    |Reduce| (['sample','fq_chunk'],SAMPE)
    |Reduce| (['sample'],CLEAN_SAM)
    |Split| ([intervals],IRTC)
    |Apply| IR
    |Reduce| (['sample'],BQSR)
    |Apply| PR
    |ReduceSplit| ([],[glm,intervals], UG)
    |Reduce| (['glm'],CV)
    |Apply| VQSR
    |Reduce| ([],CV)
    |Split| ([dbs],ANNOVAR)
    |Apply| PROCESS_ANNOVAR
    |Reduce| ([],MERGE_ANNOTATIONS)
    |Apply| SQL_DUMP
    |Apply| ANALYSIS
) 

create_dag_img()