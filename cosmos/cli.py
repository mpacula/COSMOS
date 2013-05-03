"""
Cosmos command line interface
"""
import argparse
import os,sys
from cosmos.Workflow.models import Workflow
from cosmos.utils.helpers import confirm,representsInt

def runweb(port):
    """
    Start the webserver
    """
    os.system('cosmos django runserver 0.0.0.0:{0}'.format(port))

def shell():
    """
    Open up an ipython shell with Cosmos objects preloaded
    """
    os.system('cosmos django shell_plus')

def ls(workflow_id):
    """
    List all workflows, or all stages in a workflow is a workflow.id is passed
    """
    if workflow_id:
        wf = Workflow.objects.get(pk=workflow_id)
        print wf
        print wf.describe()
        for s in wf.stages:
            print "\t{0.order_in_workflow}) {0}".format(s)
    else:
        for w in Workflow.objects.all():
            print w

def rm(workflows,prompt_confirm,stage_number,all_stages_after):
    """
    Deletes a workflow
    """
    workflows = [  Workflow.objects.get(pk=workflow) if representsInt(workflows) else Workflow.objects.get(name=workflow)
                   for workflow in workflows.split(',') ]
    for wf in workflows:
        if stage_number:
            stage = wf.stages.get(order_in_workflow=stage_number)
            if not prompt_confirm or confirm('Are you sure you want to {0}{1}?'.
                                             format(stage,' and all stages after it' if all_stages_after else ''),
                                             default=False,timeout=60):
                for s in wf.stages.filter(order_in_workflow__gt=stage.order_in_workflow-1) if all_stages_after else wf.stages.filter(order_in_workflow = stage.order_in_workflow-1):
                    s.delete()
        else:
            if not prompt_confirm or confirm('Are you sure you want to delete {0}?'.format(wf),default=False,timeout=60):
                wf.delete()

#
#def init():
#    """
#    Initializes Cosmos
#    """
#    if confirm('This will overwrite your original configuration, are you sure?',default=False):
#        os.system('cosmos django syncdb && cosmos django collectstatic')

def syncdb():
    "Sets up the SQL database"
    os.system('cosmos django syncdb')

def collectstatic():
    "Collects static files for the web interface"
    os.system('cosmos django collectstatic')
    
def resetdb():
    "DELETE ALL DATA in the database and then run a syncdb"
    os.system('cosmos django reset_db -R default')
    os.system('cosmos django syncdb --noinput')


def django(django_args):
    "Django manage.py script"
    from django.core.management import execute_from_command_line
    execute_from_command_line([sys.argv[0]]+django_args)

def main():
    parser = argparse.ArgumentParser(description='Cosmos CLI')
    subparsers = parser.add_subparsers(title="Commands", metavar="<command>")

    subparsers.add_parser('resetdb',help=resetdb.__doc__).set_defaults(func=resetdb)

    subparsers.add_parser('shell',help=shell.__doc__).set_defaults(func=shell)
#    subparsers.add_parser('init',help=init.__doc__).set_defaults(func=init)

    subparsers.add_parser('syncdb',help=syncdb.__doc__).set_defaults(func=syncdb)
    sp=subparsers.add_parser('collectstatic',help=collectstatic.__doc__).set_defaults(func=collectstatic)

    django_sp = subparsers.add_parser('django',help=django.__doc__)
    django_sp.set_defaults(func=django)
    django_sp.add_argument('django_args', nargs=argparse.REMAINDER)

    sp=subparsers.add_parser('ls',help=ls.__doc__)
    sp.set_defaults(func=ls)
    sp.add_argument('workflow_id',type=int,help="Workflow id",nargs="?")

    sp = subparsers.add_parser('rm',help=rm.__doc__)
    sp.set_defaults(func=rm)
    sp.add_argument('workflows',type=str,help="Workflow id or workflow name, can be comma separated")
    sp.add_argument('stage_number',type=int,help="Delete this stage",nargs="?")
    sp.add_argument('-a','--all_stages_after',action='store_true',help="If a stage_number is specified, delete all stages that come after that stage as well")
    sp.add_argument('-y','--prompt_confirm',action='store_false',default=True)

    runweb_sp = subparsers.add_parser('runweb',help=runweb.__doc__)
    runweb_sp.set_defaults(func=runweb)
    runweb_sp.add_argument('-p','--port',help='port to serve on',default='8080')

    a = parser.parse_args()
    kwargs = dict(a._get_kwargs())
    del kwargs['func']
    a.func(**kwargs)


