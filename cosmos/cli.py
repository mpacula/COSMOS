"""
Cosmos command line interface
"""
import argparse
import os,sys
import cosmos.session
from cosmos.Workflow.models import Workflow

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

def list():
    """
    List all workflows
    """
    for w in Workflow.objects.all():
        print w
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
    os.system('cosmos django syncdb')


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
    subparsers.add_parser('collectstatic',help=collectstatic.__doc__).set_defaults(func=collectstatic)

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


