from argh import arg,ArghParser,command,CommandError
import cosmos_session
from Workflow.models import Workflow
import os

@arg('-p','--port',help='port to serve on',default='8080')
def runweb(args):
    """
    Start the webserver
    """
    os.system('manage runserver 0.0.0.0:{0}'.format(args.port))   
     
def shell(args):
    """
    Open up an ipython shell with Cosmos objects preloaded
    """
    os.system('manage shell_plus')
    
def syncdb(args):
    "Sets up the SQL database"
    os.system('manage syncdb')
    
def resetentiredb(args):
    "DELETE ALL DATA in the database and then run a syncdb."
    os.system('manage reset_db -R default')
    os.system('manage syncdb')
    
@arg('id',help='workflow id')
@arg('-q',action="store_true",help='Queued Jobs only')  
@arg('-jid',action="store_true",help='DRMAA Job id only.  Often used with somethign like |xargs bkill')    
def jobs(args):
    jobs = Workflow.objects.get(pk=args.id).jobManager.jobAttempts.all()
    if args.q:
        jobs = jobs.filter(queue_status='queued')
    for ja in jobs:
        if args.jid:
            print ja.drmaa_jobID
        else:
            print ja
    
            
def list(args):
    """List all workflows"""
    for workflow in Workflow.objects.all():
        print workflow
    
parser = ArghParser()
parser.add_commands([runweb,shell,syncdb,resetentiredb],namespace='adm',title='Admin')
parser.add_commands([list,jobs],namespace='wf',title='Workflow')

if __name__=='__main__':
    parser.dispatch()