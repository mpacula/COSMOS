"""
WGA Workflow
"""
import argparse

from cosmos.contrib.ezflow.dag import DAG,Add
from erik.WGA.gatk import GATK_Best_Practices
from cosmos.Workflow.cli import CLI
from settings import settings
from cosmos.contrib.ezflow.tool import INPUT
from cosmos.Workflow.models import TaskFile, Workflow
import re

import json
import os

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
    Input file is a bam with properly annotated readgroups.  Note that this means the header
    is also properly annotated with the correct readgroups.
    """
    from scripts import rg_helpers
    from bam2fastq import Bam2Fastq
    rgids = list(rg_helpers.list_rgids(input_bam,settings['samtools_path']))
    print 'RGIDS:'
    print rgids

    dag = DAG(mem_req_factor=1) |Add| [ INPUT(input_bam) ]
    Bam2Fastq(dag,settings,rgids)
    dag.create_dag_img('/tmp/graph.svg')

    dag.add_to_workflow(workflow)
    workflow.run(finish=False)

    #Load Fastq Chunks
    s = workflow.stages.get(name='SplitFastq')
    inputs = []
    for t in s.tasks:
        d = {}
        d.update(t.tags)
        d['sample'] = 'NA12878'
        d['library'] = 'LIB-NA12878'
        d['platform'] = 'ILLUMINA'
        d['flowcell'] = d['rgid'][:5]
        for f in os.listdir(t.output_files[0].path):
            d2 = d.copy()
            d2['chunk'] = re.search("(\d+)\.fastq",f).group(1)
            d2['path'] = os.path.join(t.output_files[0].path.replace('/scratch/esg21/cosmos_out/Bam2Fastq3_NA12878_WGS/',''),f)
            d2['lane'] = d['rgid'][6:]
            inputs.append(INPUT(d2['path'],tags=d2))
    dag |Add| (inputs,"Load Fastq Chunks")

    #Run GATK
    GATK_Best_Practices(dag,settings)

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

    a = parser.parse_args()
    kwargs = dict(a._get_kwargs())
    del kwargs['func']
    wf = Workflow.start(**kwargs)
    a.func(workflow=wf,**kwargs)

if __name__ == '__main__':
    main()
