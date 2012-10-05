#import os,sys
#import logging
#import itertools
#logging.basicConfig(level=logging.INFO)
#
####DJANGO
#path = '/home2/erik/workspace/Cosmos'
#if path not in sys.path:
#    sys.path.append(path)
#
#os.environ['DJANGO_SETTINGS_MODULE'] = 'Cosmos.settings'
#os.environ['DRMAA_LIBRARY_PATH'] = '/opt/sge6/lib/linux-x64/libdrmaa.so'
#os.environ['SGE_ROOT'] = '/opt/sge6'
#os.environ['SGE_EXECD_PORT'] = '63232'
#os.environ['SGE_QMASTER_PORT'] = '63231'
#from django.conf import settings
####END DJANGO
#
##CONFIG
#GATK = 'java -jar /home2/erik/gatk/dist/GenomeAnalysisTK.jar'
#picard_addRG = 'java -jar /home2/erik/picard-tools-1.74/AddOrReplaceReadGroups.jar'
#
#from argh import arg,ArghParser
#from Workflow.models import SimpleWorkflow,ExecutionNode
#import re
#
#def _getOrCreateSimpleWorkflow(wfname,commands):
#    """
#    Returns workflow if it exists, otherwise creates a new one with execution nodes of commands
#    """
#    wfdefaults={
#              'output_dir':'/home2/erik/workspace/Cosmos/my_workflows/sge_out'
#             }
#    WF, just_created = SimpleWorkflow.objects.get_or_create(name=wfname,defaults=wfdefaults)
#    if just_created:
#        logging.info('Creating Workflow')
#        WF.save()
#        for i,cmd in enumerate(commands):
#            WF.addNode(ExecutionNode(command=cmd,name="{0}_job_{1}".format(wfname,i+1)))
#    else:
#        logging.info('Workflow already exists, loading it')
#    return WF
#
#def mk_output_dir(path):
#    if os.path.exists(path):   
#        if not os.path.isdir(path):
#            raise Exception('output path is a file, not a directory')
#    else:
#        os.mkdir(path)
#        logging.info('created output directory {0}'.format(path))
#
#def swapExt(filepath,newext):
#    return re.sub('\..+$',newext,filepath)
#
#fastqpy = '/home2/erik/workspace/Cosmos/my_workflows/cai/fastq.py'
#python = '/home2/erik/workspace/Cosmos/venv/bin/python'
#picard_fastq2sam  = 'java -jar /home2/erik/picard-tools-1.74/FastqToSam.jar'
#project_dir = '/2GATK/cai2'
#
#@arg('workflow_name')
#@arg('-i','--input_dir',type=str, help='input_dir')
#def splitFastqs(args):
#    """
#    splits fastq by lane
#    """
#    output_dir = os.path.join(project_dir,'splitFastqs')
#    mk_output_dir(output_dir)
#    wfname = 'splitFastq_'+args.workflow_name  
#
#    fastqs = filter(lambda f: re.search('\.fq$|\.fastq$',f),os.listdir(args.input_dir))
#    cmds = []
#    for fastq in fastqs:
#        input_file = os.path.join(args.input_dir,fastq)
#        cmds.append('{0} {1} split-fastq-by-lanes -i {2} -o {3}'.format(python, fastqpy, input_file, output_dir))        
#    
#    wf = _getOrCreateSimpleWorkflow(wfname,cmds)
#    print wf
#    wf.run()
#
#@arg('workflow_name')
#@arg('-i','--input_dir',type=str, help='input_dir')
#@arg('-d','--delete_wf_first',action='store_true',default=False, help='starts workflow over')
#def fastq2bam(args):
#    output_dir = os.path.join(project_dir,'fastq2bam')
#    mk_output_dir(output_dir)
#    wfname = 'fastq2bam_'+args.workflow_name
#    
#    fastqs = filter(lambda f: re.search('\.fq$|\.fastq$',f),os.listdir(args.input_dir))
#    cmds = []
#    
#    def aggregate(iterable,fxn):
#        """aggregates an iterable using a function"""
#        return itertools.groupby(sorted(iterable,key=fxn),fxn)
#    def fastq2sample(filename):
#        return re.search('(.+?)_',filename).group(1)
#    def fastq2lane(filename):
#        return re.search('(lane\d+)',filename).group(1)
#        
#    for sample,lanes in aggregate(fastqs,fastq2sample):
#        for lane,reads in aggregate(lanes,fastq2lane):
#            reads = [ r for r in reads ]
#            if len(reads) != 2:
#                raise Exception('expected paired end reads for each lane')
#            
#            dict = {
#                'bin' : picard_fastq2sam,
#                'fastq1' : os.path.join(args.input_dir,reads[0]),
#                'fastq2' : os.path.join(args.input_dir,reads[1]),
#                'lane' : lane,
#                'output_filepath' : os.path.join(output_dir,'{0}_{1}.bam'.format(sample,lane)),
#                'platform': 'ILLUMINA',
#                'sample' : sample
#            }
#
#            cmd = ('{bin} '
#            'FASTQ={fastq1} '
#            'FASTQ2={fastq2} '
#            'OUTPUT={output_filepath} '        
#            'READ_GROUP_NAME=flowcell1.{lane} '
#            'QUALITY_FORMAT=Standard '        
#            'SAMPLE_NAME={sample} '
#            'LIBRARY_NAME={sample}-lib-1 '
#            'PLATFORM_UNIT=NA '
#            'PLATFORM={platform} '
#            'SORT_ORDER=coordinate'
#            ).format(**dict)
#            cmds.append(cmd)
#    #print '\n'.join(cmds)
#    if args.delete_wf_first == True:
#        SimpleWorkflow.objects.get(name=wfname).delete()
#    wf = _getOrCreateSimpleWorkflow(wfname,cmds)
#    print wf
#    wf.run()
#
##    
##    java -jar ~/picard-tools-1.74/FastqToSam.jar F1=N12-141_R2.fq F2=N12-141_R2.fq V=Standard O=N12-141.bam SM=N12-141 LB=LB_N12-141 PU=run_barcode.lane PL=Illumina
#    
#    
#    
#
#
#parser = ArghParser()
#parser.add_commands([fastq2bam,splitFastqs])
#
#if __name__=='__main__':
#    parser.dispatch()
