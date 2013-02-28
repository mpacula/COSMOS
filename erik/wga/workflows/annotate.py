from cosmos.contrib.ezflow.dag import DAG, Map, Reduce, Split, ReduceSplit, Add
from cosmos.contrib.ezflow.flow import SubWorkFlow
from wga import settings
from subprocess import Popen,PIPE
from wga.tools.annotate import AnnovarExt_DownDB

class Annotate(SubWorkFlow):
    """
    Annotates with all databases
    """
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
        cmd = 'annovarext listdbs'
        dbs = Popen(cmd.split(' '),stdout=PIPE).communicate()[0]
        if len(dbs) < 10:
            raise Exception, "could not list databases"
        dag |Add| [ AnnovarExt_DownDB(tags={'build':'hg19','dbname':db}) for db in dbs.split('\n') if db != '' ]

