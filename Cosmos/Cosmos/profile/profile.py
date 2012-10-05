import subprocess,time,sys,re,os,sqlite3,json

"""
    Sample some process statistics from /proc/[pid]/status
     
    polls these fields from /proc/pid/stat:
    see "man proc" for more info, or see read_man_proc.py
    minflt, cminflt, majflt, utime, stime, cutime, cstime, priority, nice, num_threads, rsslim, exit_signal, delayacct_blkio_ticks

    polls these fields from /proc/pid/status:
    * FDSize: Number of file descriptor slots currently allocated.
    * VmPeak: Peak virtual memory size.
    * VmSize: Virtual memory size.
    * VmLck: Locked memory size (see mlock(3)).
    * VmHWM: Peak resident set size ("high water mark").
    * VmRSS: Resident set size.
    * VmData, VmStk, VmExe: Size of data, stack, and text segments.
    * VmLib: Shared library code size.
    * VmPTE: Page table entries size (since Linux 2.6.10).
    
    And returns:
    MAX(FDSize), AVG(FDSize), MAX(VmPeak), AVG(VmSize), MAX(VmLck), AVG(VmLck), AVG(VmRSS), AVG(VmData), MAX(VmData), AVG(VmLib), MAX(VmPTE), AVG(VmPTE) 
"""

class Profile:

    fields_to_get = ['FDSize','VmPeak','VmSize','VmLck','VmHWM','VmRSS','VmData','VmLib','VmPTE','voluntary_ctxt_switches','nonvoluntary_ctxt_switches']
    how_to_analyze = {'max' : ['VmPeak','VmHWM'],
                      'mean':['FDSize','VmSize','VmLck','VmRSS','VmData','VmLib','VmPTE']}
    proc = None #the main subprocess object
    
    @property
    def all_pids(self):
        """This main process and all of its descendant's pids"""
        return self.and_descendants(os.getpid())
    
    def __init__(self,command,poll_interval=1):
        self.command = command
        self.poll_interval = poll_interval
        self.conn = sqlite3.connect(':memory:')
        self.c = self.conn.cursor()
        sqfields = ', '.join(map(lambda x: x + ' INTEGER', self.fields_to_get))
        self.c.execute("CREATE TABLE polls (pid INTEGER, name TEXT, {0})".format(sqfields))
        
    def _unnest(self,a_list):
        """
        unnests a list
        
        .. example::
        >>> _unnest([1,2,[3,4],[5]])
        >>> [1,2,3,4,5]
        """
        return [ item for items in a_list for item in items ]
    
    def and_descendants(self,pid):
        """Returns a list of this pid and all of its descendent process (children's children, etc) ids"""
        p = subprocess.Popen('ps h --ppid {0} -o pid'.format(pid),shell=True,stdout=subprocess.PIPE)
        children = map(lambda x: x.strip(),filter(lambda x: x!='',p.communicate()[0].strip().split('\n')))
        
        if len(children) == 0:
            return [pid]
        else: 
            return [pid] + self.unnest([ self.and_descendants(int(child)) for child in children ])
    
    def insert_dict(self,table):
        """
        Insert a dictionary of data into table
        """
        
    
    
    def run(self):
        """Runs a process and records the memory usage of it and all of its descendants"""
        self.proc = subprocess.Popen(self.command, shell=True)
        while True:
            for pid,name,status in self.profile_all_procs():
                keys=','.join(map(lambda x:x[0],status))
                vals = map(lambda x:x[1],status)      
                self.c.execute("insert into polls(pid,name,{keys}) values({q})".format(keys=keys,
                                                                              q=', '.join(['?']*(len(vals)+2)),
                                                                              ),
                                                                              [pid,name] + vals)
            time.sleep(1)
            if self.proc.poll() != None:
                self.finish()
    
    def profile_all_procs(self):
        """Return a dictionary of process names and their current resource usage"""
        return filter(lambda x: x,[ self.profile_proc(pid) for pid in self.all_pids ])
    
    def profile_proc(self,pid):
        """
        Gets resource information of a process from /proc/[pid]/status and /proc/[pid]/stat
        :returns: a tuple that looks like (self.fields_to_get found in /proc/[pid]/status, name of the pid, [(status field, status value)])
        """
        def line2tuple(l):
            m = re.search(r"\s*(.+):\s*(.+)\s*",l)
            return m.group(1),m.group(2)
        def parseVal(key,val):
            "Remove kB and return"
            return key,int(val) if val[-2:] != 'kB' else int(val[0:-3])
        try:
            with open('/proc/{0}/status'.format(pid),'r') as f:
                status_all = map(line2tuple,f.readlines())
                name = filter(lambda x: x[0] == 'Name',status_all)[0][1]
                status_filtered = filter(lambda x: x[0] in self.fields_to_get,status_all)
                status_parsed = map(lambda x: parseVal(*x),status_filtered)
                return pid,name,status_parsed
        except IOError:
            return
        
    def analyze_records(self):
#        for pid in self.records:
        self.c.execute('SELECT name,pid, MAX(FDSize), AVG(FDSize), MAX(VmPeak), AVG(VmSize), MAX(VmLck), AVG(VmLck), AVG(VmRSS), AVG(VmData), MAX(VmData), AVG(VmLib), MAX(VmPTE), AVG(VmPTE) FROM polls GROUP BY pid,name')
        keys = [ x[0] for x in self.c.description]
        print [ json.dumps(dict(zip(keys,vals)),sort_keys=True) for vals in self.c ]
    
    def finish(self):
        self.analyze_records()
        sys.exit(self.proc.poll())

if __name__ == '__main__':
    profile = Profile('wc -l ~/Downloads/Revolution.2012.S01E01.720p.HDTV.X264-DIMENSION.mkv')
    profile = Profile('sleep 3')
    profile.run()