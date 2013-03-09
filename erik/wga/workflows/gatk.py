from cosmos.contrib.ezflow.dag import DAG, Map, Reduce, Split, ReduceSplit, Add
from wga.tools import gatk,picard,bwa

def GATK_Best_Practices(dag,wga_settings):
    """
    Maps GATK best practices to dag's last_tools
    """

    parameters = {
        'ALN': { 'q': 5 },
    }

    # Tags
    intervals = ('interval',range(1,23)+['X','Y'])
    glm = ('glm',['SNP','INDEL'])
    dbs = ('database',['1000G','PolyPhen2','COSMIC','ENCODE'])

    dag = (dag
           |Map| bwa.ALN
           |Reduce| (['sample','flowcell','lane','chunk'],bwa.SAMPE)
           |Map| picard.CLEAN_SAM
           |Map| picard.SORT_BAM
           |Map| (picard.INDEX_BAM,'Index Cleaned BAMs')
           |Reduce| (['sample','lane'],gatk.BQSR)
           |Map| gatk.PR
           |Reduce| ([],picard.MARK_DUPES)
           |Map| (picard.INDEX_BAM,'Index Deduped')
           |Split| ([intervals],gatk.RTC)
           |Map| gatk.IR
           |ReduceSplit| ([],[glm,intervals], gatk.UG)
           |Reduce| (['glm'],gatk.CV,'Combine into SNP and INDEL vcfs')
           |Map| gatk.VQSR
           |Map| gatk.Apply_VQSR
           |Reduce| ([],gatk.CV,"Combine into Master vcf")
           # |Split| ([dbs],annotate.ANNOVAR)
           # |Workflow| annotate.PROCESS_ANNOVAR
           # |Reduce| ([],annotate.MERGE_ANNOTATIONS)
           # |Workflow| annotate.SQL_DUMP
           # |Workflow| annotate.ANALYSIS
    )
    dag.configure(wga_settings,parameters)