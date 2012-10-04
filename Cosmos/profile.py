import subprocess,time,sys,re,os

"""
    Sample some process statistics from /proc/[pid]/status
 
    NOTE: this function is called after poll() determines that this
    PID is the JID we think it is. HOWEVER, that does very little for
    us--the correct PID may have died between poll()'s verification
    and this function call.  Hence the resources polled here may
    come from the wrong process.  This mistake can happen at most
    once, so if we need to be completely safe, we can ignore the
    final measurement.  In reality, this situation is probably so
    rare (and the error introduced by one incorrect resource
    measurement so insignificant) that we can safely ignore this.
    
    saving these fields from /proc/status:
    * FDSize: Number of file descriptor slots currently allocated.
    * VmPeak: Peak virtual memory size.
    * VmSize: Virtual memory size.
    * VmLck: Locked memory size (see mlock(3)).
    * VmHWM: Peak resident set size ("high water mark").
    * VmRSS: Resident set size.
    * VmData, VmStk, VmExe: Size of data, stack, and text segments.
    * VmLib: Shared library code size.
    * VmPTE: Page table entries size (since Linux 2.6.10).
    
    .. note:: All entries for all descendent processes are summed up as well.  I am not sure if this makes
    sense for all these fields
"""

def unnest(a_list):
    """
    unnests a list
    
    .. example::
    >>> unnest([1,2,[3,4],[5]])
    >>> [1,2,3,4,5]
    """
    return [ item for items in a_list for item in items ]

def and_descendants(pid):
    """Returns a list of this pid and all of its descendent process (children's children, etc) ids"""
    p = subprocess.Popen('ps h --ppid {0} -o pid'.format(pid),shell=True,stdout=subprocess.PIPE)
    children = map(lambda x: x.strip(),filter(lambda x: x!='',p.communicate()[0].strip().split('\n')))
    
    if len(children) == 0:
        return [pid]
    else: 
        return [pid] + unnest([ and_descendants(int(child)) for child in children ])


class Profile:
    fields_to_get = ['FDSize','VmPeak','VmSize','VmLck','VmHWM','VmRSS','VmData','VmLib','VmPTE']
    proc = None #the main subprocess object
    command = None
    records = None # a list of polling information for all jobs per poll_interval
    poll_interval = None
    
    def __init__(self,command,poll_interval=1):
        self.command = command
        self.poll_interval = poll_interval
        self.records = []
    
    @property
    def all_pids(self):
        """This main process and all of its descendant's pids"""
        return and_descendants(self.proc.pid)
    
    def run(self):
        """Runs a process and records the memory usage of it and all of its descendants"""
        self.proc = subprocess.Popen(self.command, shell=True)
        while True:
            self.records.append(self.profile_all_procs())
            time.sleep(1)
            if self.proc.poll() != None:
                self.finish()
    
    def profile_all_procs(self):
        """Return a dictionary of process names and their current resource usage"""
        return [ self.profile_proc(pid) for pid in self.all_pids ]
    
    def profile_proc(self,pid):
        """
        Return the name of a process and a dictionary of self.fields_to_get found in /proc/[pid]/status
        """
        def line2tuple(l):
            m = re.search(r"\s*(.+):\s*(.+)\s*",l)
            return (m.group(1),m.group(2))
        try:
            with open('/proc/{0}/status'.format(pid),'r') as f:
                status = dict(map(line2tuple,f.readlines()))
                return status['Name'],dict((key,status[key]) for key in self.fields_to_get)
        except IOError:
            return
    
    def finish(self):
        print profile.records
        sys.exit(self.proc.poll())

if __name__ == '__main__':
    profile = Profile('wc -l ~/Downloads/Revolution.2012.S01E01.720p.HDTV.X264-DIMENSION.mkv')
    profile = Profile('sleep 3')
    profile.run()