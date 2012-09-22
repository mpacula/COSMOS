from argh import arg,ArghParser,command,CommandError
import cosmos_session
from Workflow.models import Workflow
import os

@arg('-n','--name',help="name of workflow to terminate")
@arg('-id',type=str, help='id for workflow to terminate')
def terminate(args):
    if (args.name and args.id) or (not args.name and not args.id):
        raise CommandError('please choose a name OR an id')
    if args.name:
        wf = Workflow.objects.get(name=args.name)
    elif args.id:
        wf = Workflow.objects.get(pk=args.id)
    wf.terminate()
    print "Told workflow {0} to terminate".format(wf)


@arg('-p','--port',help='port to serve on',default='8080')
def runweb(args):
    os.system('./manage.py runserver 0.0.0.0:{0}'.format(args.port))   
     
def workflows(args):
    for workflow in Workflow.objects.all():
        print workflow
        

parser = ArghParser()
parser.add_commands([terminate,runweb,workflows])

if __name__=='__main__':
    parser.dispatch()