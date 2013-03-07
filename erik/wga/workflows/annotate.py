from cosmos.contrib.ezflow.dag import DAG, Map, Reduce, Split, ReduceSplit, Add
from cosmos.contrib.ezflow.flow import SubWorkFlow
from subprocess import Popen,PIPE
from wga.tools import annotation

def get_db_names():
    cmd = 'annovarext listdbs'
    dbs = Popen(cmd.split(' '),stdout=PIPE).communicate()[0]
    if len(dbs) < 10:
        raise Exception, "could not list databases"
    return [ db for db in dbs.split('\n') if db != '' ]

class Annotate(SubWorkFlow):
    """
    Annotates with all databases
    """
    def flow(self,dag):
        """
        No inputs or outputs, just downloads databases
        """
        ( dag
          |Split| ( [('build',['hg19']),
                      ('dbname',get_db_names()) ],
                    annotation.Anno )
          |Reduce| ([],annotation.MergeAnno)
        )


    def cli(self,parser):
        pass

class DownDBs(SubWorkFlow):
    """
    Downloads all available databases
    """
    def flow(self,dag):
        """
        No inputs or outputs, just downloads databases
        """
        dag |Add| [ annotation.DownDB(tags={'build':'hg19','dbname':db}) for db in get_db_names() ]

