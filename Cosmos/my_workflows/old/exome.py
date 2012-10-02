import os,sys
import logging

logging.basicConfig(level=logging.INFO)

###DJANGO
path = '/home2/erik/workspace/Cosmos'
if path not in sys.path:
    sys.path.append(path)

os.environ['DJANGO_SETTINGS_MODULE'] = 'Cosmos.settings'
os.environ['DRMAA_LIBRARY_PATH'] = '/opt/sge6/lib/linux-x64/libdrmaa.so'
os.environ['SGE_ROOT'] = '/opt/sge6'
os.environ['SGE_EXECD_PORT'] = '63232'
os.environ['SGE_QMASTER_PORT'] = '63231'
from django.conf import settings
###END DJANGO

#CONFIG
GATK = 'java -jar /home2/erik/gatk/dist/GenomeAnalysisTK.jar'
picard_addRG = 'java -jar /home2/erik/picard-tools-1.74/AddOrReplaceReadGroups.jar'

from argh import arg,ArghParser
from Workflow.models import Batch,Node
import re


def _getSimpleWorkflow(wfname,commands):
    """
    Returns workflow if it exists, otherwise creates a new one with execution nodes of commands
    """
    wfdefaults={
              'output_dir':'/home2/erik/workspace/Cosmos/my_workflows/sge_out'
             }
    WF, just_created = Batch.objects.get_or_create(name=wfname,defaults=wfdefaults)
    if just_created:
        logging.info('Creating Workflow')
        WF.save()
        for i,cmd in enumerate(commands):
            WF.addNode(Node(command=cmd,name="{0}_job_{1}".format(wfname,i+1)))
    else:
        logging.info('Workflow already exists, loading it')
    return WF

@arg('workflow_name')
def status(args):
    print Batch.objects.get(name=args.workflow_name)

@arg('workflow_name')
def sampe(args):
    with open('align_cmds.txt','rb') as f:
        cmds = f.readlines()
    wfname = 'sampe_'+args.workflow_name        
    #getSimpleWorkflow(wfname,cmds)
    print WF 
    #        
    ##WF.run()

@arg('workflow_name')
@arg('-d',action='store_true',default=False,help='delete workflow history and start over')
def addRG(args):
    """
    RGID=String	Read Group ID Default value: 1. This option can be set to 'null' to clear the default value.
    RGLB=String	Read Group Library Required.
    RGPL=String	Read Group platform (e.g. illumina, solid) Required.
    RGPU=String	Read Group platform unit (eg. run barcode) Required.
    RGSM=String	Read Group sample name Required.
    RGCN=String	Read Group sequencing center name Default value: null.
    RGDS=String	Read Group description Default value: null.
    RGDT=Iso8601Date	Read Group run date Default value: null.
    """
    input_dir = '/2GATK/cai/alignment'
    output_dir='/2GATK/cai/alignment2'
    cmds = []
    for f in filter(lambda x:re.search('bam',x),os.listdir(input_dir)):
        print f
        m = re.search('(?P<sample>.+)_(?P<lane>lane[\d]+)',f)
        d = { 'addRG' : picard_addRG,
            'sample' : m.group('sample'),
            'lane' : m.group('lane'),
            'input' : os.path.join(input_dir,f),
            'platform' : 'ILLUMINA',
             \
       \
            }
        d['output'] = os.path.join(output_dir,"{sample}_{lane}_addRG.bam".format(**d))
        cmd = ('{addRG} '
        'VALIDATION_STRINGENCY=LENIENT '
        'SORT_ORDER=coordinate '
        'I={input} '
        'O={output} '
        'RGID=flowcell1.{lane} '
        'RGLB={sample}-lib-1 '
        'RGPL={platform} '
        'RGPU=NA '
        'RGSM={sample}').format(**d)
        cmds.append(cmd)
    wfname = 'addRG_'+args.workflow_name
    if args.d:
        Batch.objects.get(name=wfname).delete()
    wF = _getSimpleWorkflow(wfname,cmds)
    print wF
    wF.run()
    
parser = ArghParser()
parser.add_commands([status,sampe,addRG])

if __name__=='__main__':
    parser.dispatch()