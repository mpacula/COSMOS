import argparse
import cosmos.session
from cosmos.Workflow.models import Workflow
import os

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
    :return:
    """
    for w in Workflow.objects.all():
        print w

def syncdb():
    "Sets up the SQL database"
    os.system('manage syncdb')
    
def resetentiredb():
    "DELETE ALL DATA in the database and then run a syncdb"
    os.system('manage reset_db -R default')
    os.system('manage syncdb')



def main():
    parser = argparse.ArgumentParser(description='Cosmos CLI')
    subparsers = parser.add_subparsers(help='')

    subparsers.add_parser('resetentiredb',description=resetentiredb.__doc__).set_defaults(func=resetentiredb)

    subparsers.add_parser('shell',description=shell.__doc__).set_defaults(func=shell)

    subparsers.add_parser('syncdb',description=syncdb.__doc__).set_defaults(func=syncdb)

    wf_sp = subparsers.add_parser('list',description=list.__doc__).set_defaults(func=list)

    runweb_sp = subparsers.add_parser('runweb',description=runweb.__doc__)
    runweb_sp.set_defaults(func=runweb)
    runweb_sp.add_argument('-p','--port',help='port to serve on',default='8080')

    a = parser.parse_args()
    kwargs = dict(a._get_kwargs())
    del kwargs['func']
    a.func(**kwargs)


