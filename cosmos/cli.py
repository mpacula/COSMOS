import argparse
import cosmos.session
from cosmos.Workflow.models import Workflow
import os,sys
from cosmos import manage

def runweb(port):
    """
    Start the webserver
    """
    os.system('manage runserver 0.0.0.0:{0}'.format(port))

def shell():
    """
    Open up an ipython shell with Cosmos objects preloaded
    """
    os.system('manage shell_plus')

def list():
    """
    List all workflows
    """
    for w in Workflow.objects.all():
        print w

def syncdb():
    "Sets up the SQL database"
    os.system('manage syncdb')
    
def resetdb():
    "DELETE ALL DATA in the database and then run a syncdb"
    os.system('manage reset_db -R default')
    os.system('manage syncdb')

def django(django_args=[]):
    "Django manage.py script"
    from django.core.management import execute_from_command_line
    execute_from_command_line([sys.argv[0]]+django_args)

def main():
    parser = argparse.ArgumentParser(description='Cosmos CLI')
    subparsers = parser.add_subparsers(title="Commands", metavar="<command>")

    subparsers.add_parser('resetdb',help=resetdb.__doc__).set_defaults(func=resetdb)

    subparsers.add_parser('shell',help=shell.__doc__).set_defaults(func=shell)

    subparsers.add_parser('syncdb',help=syncdb.__doc__).set_defaults(func=syncdb)

    django_sp = subparsers.add_parser('django',help=django.__doc__)
    django_sp.set_defaults(func=django)
    django_sp.add_argument('django_args', nargs=argparse.REMAINDER)

    subparsers.add_parser('list',help=list.__doc__).set_defaults(func=list)

    runweb_sp = subparsers.add_parser('runweb',help=runweb.__doc__)
    runweb_sp.set_defaults(func=runweb)
    runweb_sp.add_argument('-p','--port',help='port to serve on',default='8080')

    a = parser.parse_args()
    kwargs = dict(a._get_kwargs())
    del kwargs['func']
    a.func(**kwargs)


