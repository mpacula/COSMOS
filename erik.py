from argh import arg, ArghParser, command
import os

all_dbs = ['Workflow','JobManager']

def run(cmd):
    print 'executing %s'%cmd
    os.system(cmd)

@arg('dbname',help='db name')
def cleardb(args):
    run('./manage.py sqlreset %s | ./manage.py dbshell'%args.dbname)
    
@arg('dbname',help="set to A to reset ALL Dbs")
def resetdb(args):
    dbs = [args.dbname]
    if args.dbname == 'A':
        dbs = all_dbs
    
    for db in dbs:
        run('./manage.py sqlclear %s | ./manage.py dbshell' % db)
    run('./manage.py syncdb')

@command
def serve():
    run('./manage.py runserver 0.0.0.0:8080')
    
parser = ArghParser()
parser.add_commands([cleardb,resetdb,serve])

if __name__=='__main__':
    parser.dispatch()