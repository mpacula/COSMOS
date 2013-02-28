"""
WGA Workflow
"""
import argparse
import json

from cosmos.contrib.ezflow.dag import DAG,Add,SWF
from cosmos.contrib.ezflow.tool import INPUT
from erik.wga.workflows.gatk import GATK_Best_Practices
from erik.wga.workflows.annotate import DownDBs
from erik.wga.workflows.bam2fastq import Bam2Fastq
from cosmos.Workflow.cli import CLI
from cosmos.Workflow.models import TaskFile, Workflow
from scripts import rg_helpers
from settings import settings

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
    inputs = [ INPUT(taskfile=TaskFile(name='fastq.gz',path=i['path'],fmt='fastq.gz'),tags=i) for i in input_json ]

    #Create DAG
    dag = DAG(mem_req_factor=1) |Add| inputs
    GATK_Best_Practices(dag,settings)
    dag.create_dag_img('/tmp/graph.svg')

    dag.add_to_workflow(workflow)
    workflow.run()

def bam(workflow,input_bam,**kwargs):
    """
    Input file is a bam with properly annotated readgroups.  Note that this also
    means the bam header is also properly annotated with the correct readgroups.
    """

    rgids = list(rg_helpers.list_rgids(input_bam,settings['samtools_path']))
    print 'RGIDS:'
    print rgids

    dag = DAG(mem_req_factor=1) |Add| [ INPUT(input_bam) ]

    #Run bam2fastq
    Bam2Fastq(workflow,dag,settings,rgids)
    dag.add_to_workflow(workflow)
    workflow.run(finish=False)

    #Run GATK
    GATK_Best_Practices(dag,settings)
    dag.add_to_workflow(workflow)
    workflow.run()

def downdbs(workflow,**kwargs):
    dag = DAG() |SWF| DownDBs

    dag.add_to_workflow(workflow)
    workflow.run()


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

    downdbs_sp = subparsers.add_parser('downdbs',help=DownDBs.__doc__)
    CLI.add_default_args(downdbs_sp)
    downdbs_sp.set_defaults(func=downdbs)

    a = parser.parse_args()
    kwargs = dict(a._get_kwargs())
    del kwargs['func']
    wf = Workflow.start(**kwargs)
    a.func(workflow=wf,**kwargs)

if __name__ == '__main__':
    main()
