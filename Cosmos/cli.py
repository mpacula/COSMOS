from argh import arg,ArghParser,command,CommandError
import cosmos_session
from Workflow.models import Workflow
import os


def run(cmd):
    print 'executing %s'%cmd
    os.system(cmd)

@arg('id',type=str, help='id for workflow to terminate')
def stop(args):
    if args.id is None:
        raise CommandError('please choose a name OR an id')
    wf = Workflow.objects.get(pk=args.id)
    print "Telling workflow {0} to terminate".format(wf)
    wf.terminate()

    #lsf qdel all: bjobs|cut -d " " -f 1 -|sed 1d|xargs -t -L 1 qdel

@arg('-p','--port',help='port to serve on',default='8080')
def runweb(args):
    os.system('manage runserver 0.0.0.0:{0}'.format(args.port))   
    
@arg('-p','--port',help='port to serve on',default='8080')
def runweb(args):
    os.system('manage runserver 0.0.0.0:{0}'.format(args.port))   
     
     
def workflows(args):
    for workflow in Workflow.objects.all():
        print workflow
        
def shell(args):
    os.system('manage shell_plus')

@arg('-y',action='store_true',default=False,help="WARNING: Resets the Workflow sql database.  Does not delete output files.")
def resetdb(args):
    if args.y:
        dbs = ['Workflow','JobManager']
        for db in dbs:
            run('manage sqlclear %s | manage dbshell' % db)
        run('manage syncdb')
    else:
        print 'You must use -y so I know you\'re sure'

parser = ArghParser()
parser.add_commands([stop,runweb,workflows,shell,resetdb])

if __name__=='__main__':
    parser.dispatch()