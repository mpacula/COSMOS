from cosmos.contrib.ezflow.dag import DAG, Apply, Reduce, Split, ReduceSplit, Add
from tools import gatk,picard,bwa

def GATK_Best_Practices(dag,settings):
    """
    Applys GATK best practices to dag's last_tools
    """

    parameters = {
        'ALN': { 'q': 5 },
    }

    # Tags
    intervals = ('interval',range(1,23)+['X','Y'])
    glm = ('glm',['SNP','INDEL'])
    dbs = ('database',['1000G','PolyPhen2','COSMIC','ENCODE'])

    dag = (dag
           |Apply| bwa.ALN
           |Reduce| (['sample','flowcell','lane','chunk'],bwa.SAMPE)
           |Apply| picard.CLEAN_SAM
           |Reduce| (['sample'],picard.MERGE_SAMS)
           |Apply| picard.INDEX_BAM
           |Split| ([intervals],gatk.RTC)
           |Apply| gatk.IR
           |Reduce| (['sample'],gatk.BQSR)
           |Apply| gatk.PR
           |ReduceSplit| ([],[glm,intervals], gatk.UG)
           |Reduce| (['glm'],gatk.CV)
           |Apply| gatk.VQSR
           |Apply| gatk.Apply_VQSR
           |Reduce| ([],gatk.CV,"CV 2")
              # |Split| ([dbs],annotate.ANNOVAR)
              # |Apply| annotate.PROCESS_ANNOVAR
              # |Reduce| ([],annotate.MERGE_ANNOTATIONS)
              # |Apply| annotate.SQL_DUMP
              # |Apply| annotate.ANALYSIS
    )
    dag.configure(settings,parameters)