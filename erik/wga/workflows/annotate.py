from cosmos.contrib.ezflow.dag import DAG, Map, Reduce, Split, ReduceSplit, Add
from cosmos.contrib.ezflow.flow import SubWorkFlow
from wga import settings
from subprocess import Popen,PIPE
from wga.tools import annotate

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
                      annotate.AnnovarExt_Anno )
          |Reduce| ([],annotate.AnnovarExt_Merge)
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
        dag |Add| [ annotate.AnnovarExt_DownDB(tags={'build':'hg19','dbname':db}) for db in get_db_names() ]

