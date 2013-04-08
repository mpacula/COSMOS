"""
WGA Workflow
"""
import argparse
import json
import sys

from cosmos.contrib.ezflow.dag import DAG,Add,SWF,Map
from cosmos.contrib.ezflow.tool import INPUT
from erik.wga.workflows.gatk import GATK_Best_Practices
from erik.wga.workflows import annotate
from erik.wga.tools import annotation
from erik.wga.workflows.bam2fastq import Bam2Fastq
from cosmos.Workflow.cli import CLI
from cosmos.Workflow.models import TaskFile, Workflow
from scripts import rg_helpers
import wga_settings
from cosmos import session

wga_settings = wga_settings.wga_settings
session.get_drmaa_native_specification = wga_settings['get_drmaa_native_specification']

###############################
# Alignment and Variant Calling
###############################

def json_(workflow,input_dict,**kwargs):
    """
    Input file is a json of the following format:

    [
        {
            'lane': 001,
            'chunk': 001,
            'library': 'LIB-1216301779A',
            'sample': '1216301779A',
            'platform': 'ILLUMINA',
            'flowcell': 'C0MR3ACXX'
            'pair': 0, #0 or 1
            'path': '/path/to/fastq'
        },
        {..}
    ]
    """
    input_json = json.load(open(input_dict,'r'))
    inputs = [ INPUT(name='fastq.gz',path=i['path'],fmt='fastq.gz',tags=i) for i in input_json ]

    #Create DAG
    dag = DAG(mem_req_factor=1) |Add| inputs
    GATK_Best_Practices(dag,wga_settings)
    dag.create_dag_img('/tmp/graph.svg')

    dag.add_run(workflow)

def bam(workflow,input_bam,**kwargs):
    """
    Input file is a bam with properly annotated readgroups.  Note that this also
    means the bam header is also properly annotated with the correct readgroups.
    """

    rgids = list(rg_helpers.list_rgids(input_bam,wga_settings['samtools_path']))
    print 'RGIDS:'
    print rgids

    dag = DAG() |Add| [ INPUT(input_bam,tags={'input':input_bam}) ]

    #Run bam2fastq
    Bam2Fastq(workflow,dag,wga_settings,rgids)

    #Run GATK
    GATK_Best_Practices(dag,wga_settings)

    dag.add_run(workflow)


###############################
# Annotation
###############################

def downdbs(workflow,**kwargs):
    """
    Download all annotation databases
    """
    dag = DAG() |SWF| annotate.DownDBs

    dag.add_run(workflow)


def anno(workflow,input_files,tsv=False,**kwargs):
    """
    Annotate a vcf file
    """

    print >> sys.stderr, 'annotating {0}'.format(', '.join(input_files))

    dag = DAG()

    if tsv:
        dag |Add| [ INPUT(i) for i in input_files ]
    else:
        (dag
         |Add| [ INPUT(i) for i in input_files ]
         |Map| annotation.SetID
         |Map| annotation.Vcf2Anno_in
         )

    dag |SWF| annotate.Annotate

    dag.add_run(workflow)


def main():
    parser = argparse.ArgumentParser(description='WGA')
    subparsers = parser.add_subparsers(title="Commands", metavar="<command>")

    json_sp = subparsers.add_parser('json',help=json_.__doc__)
    CLI.add_default_args(json_sp)
    json_sp.set_defaults(func=json_)
    json_sp.add_argument('-i','--input_dict',type=str,help='Inputs, see script comments for format.',required=True)

    bam_sp = subparsers.add_parser('bam',help=bam.__doc__)
    CLI.add_default_args(bam_sp)
    bam_sp.add_argument('-i','--input_bam',required=True)
    bam_sp.set_defaults(func=bam)

    downdbs_sp = subparsers.add_parser('downdbs',help=downdbs.__doc__)
    CLI.add_default_args(downdbs_sp)
    downdbs_sp.set_defaults(func=downdbs)

    anno_sp = subparsers.add_parser('anno',help=annotate.Annotate.__doc__)
    CLI.add_default_args(anno_sp)
    anno_sp.add_argument('-f','--format',type=str,default='vcf',help='vcf or tsv.  If tsv: Input file is already a tsv file with ID as the 5th column')
    anno_sp.add_argument('input_files',help="input vcf files", nargs='+')
    anno_sp.set_defaults(func=anno)

    a = parser.parse_args()
    kwargs = dict(a._get_kwargs())
    del kwargs['func']
    wf = Workflow.start(**kwargs)
    a.func(workflow=wf,**kwargs)

if __name__ == '__main__':
    main()
