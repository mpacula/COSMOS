#!/usr/bin/env python

import drmaa
import os

def main():
    """Submit a job.
    Note, need file called sleeper.sh in current directory.
    """
    s = drmaa.Session()
    s.initialize()

    print 'Creating job template'
    jt = s.createJobTemplate()
    jt.remoteCommand = '/home2/erik/gatk/tools/bwa-0.6.2/bwa'
    jt.args = ['sampe']
    jt.nativeSpecification = '-shell yes -b yes'
    jt.workingDirectory = os.getcwd()
    jt.joinFiles=True

    jobid = s.runJob(jt)
    print 'Your job has been submitted with id ' + jobid

    print 'Cleaning up'
    s.deleteJobTemplate(jt)
    s.exit()
    
if __name__=='__main__':
    main()
    
#JobInfo(jobId='108', hasExited=True, hasSignal=False, terminatedSignal='SIGunknown signal', hasCoreDump=False, wasAborted=False, exitStatus=0, resourceUsage={'exit_status': '0.0000', 'ru_inblock': '0.0000', 'io': '0.0000', 'acct_maxvmem': '0.0000', 'ru_nvcsw': '2.0000', 'maxvmem': '0.0000', 'ru_isrss': '0.0000', 'ru_stime': '0.0080', 'ru_nsignals': '0.0000', 'priority': '0.0000', 'mem': '0.0000', 'ru_nivcsw': '0.0000', 'acct_iow': '0.0000', 'acct_io': '0.0000', 'acct_cpu': '0.0200', 'acct_mem': '0.0000', 'iow': '0.0000', 'start_time': '1345219771.0000', 'ru_msgsnd': '0.0000', 'ru_wallclock': '0.0000', 'ru_minflt': '1612.0000', 'submission_time': '1345219756.0000', 'ru_utime': '0.0120', 'ru_oublock': '24.0000', 'ru_nswap': '0.0000', 'ru_majflt': '0.0000', 'signal': '0.0000', 'vmem': '0.0000', 'ru_ixrss': '0.0000', 'ru_ismrss': '0.0000', 'end_time': '1345219771.0000', 'ru_idrss': '0.0000', 'ru_maxrss': '4972.0000', 'ru_msgrcv': '0.0000', 'cpu': '0.0200'})