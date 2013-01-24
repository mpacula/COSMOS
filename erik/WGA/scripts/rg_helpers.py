import argparse
from subprocess import Popen,PIPE
import re
import logging
def rg2rgid(readgroup):
    return re.search('\tID:(.+?)\t',readgroup).group(1)

def list_rgids(input_filename,samtools_path,**kwargs):
    """
    Lists read group ids in input_filename
    """
    p = Popen('{0} view -H {1}'.format(samtools_path,input_filename).split(' '),stdout=PIPE,stderr=PIPE)
    header = p.stdout.readlines()
    cmd = '{0} view -h {1}'.format(samtools_path,input_filename)
    logging.info('Getting readgroups via cmd: {0}'.format(cmd))
    view_proc = Popen(cmd.split(' '),stdout=PIPE,stderr=PIPE)
    rgids = map(rg2rgid,filter(lambda l:re.match('@RG',l),header))
    for rgid in rgids: yield rgid

def rg_header(input_filename,rgid,samtools_path,**kwargs):
    """
    Filters out all readgroups in the readgroup header that do not match rgid
    """
    p = Popen('{0} view -H {1}'.format(samtools_path,input_filename).split(' '),stdout=PIPE,stderr=PIPE)
    header = p.stdout.readlines()
    new_header = filter(lambda l:re.search('@RG.*\tID:{0}\t'.format(rgid),l) or not re.match('@RG',l),header) #filter out other readgroups
    yield ''.join(new_header).strip()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="Commands", metavar="<command>")
    list_rgids_sp = subparsers.add_parser('list_rgids',help=list_rgids.__doc__)
    list_rgids_sp.set_defaults(func=list_rgids)
    list_rgids_sp.add_argument('-i','--input_filename',type=str,help="Path to the bam file",required=True)
    list_rgids_sp.add_argument('-s','--samtools_path',type=str,help="Path to samtools binary",required=True)

    rg_header_sp = subparsers.add_parser('rg_header',help=rg_header.__doc__)
    rg_header_sp.set_defaults(func=rg_header)
    rg_header_sp.add_argument('-i','--input_filename',type=str,help="Path to the bam file",required=True)
    rg_header_sp.add_argument('-s','--samtools_path',type=str,help="Path to samtools binary",required=True)
    rg_header_sp.add_argument('-r','--rgid',type=str,help="Readgroup ID to filter for",required=True)
    opts = parser.parse_args()
    kwargs = dict(opts._get_kwargs())
    del kwargs['func']
    r = opts.func(**kwargs)
    if r:
        for x in r:
            print x