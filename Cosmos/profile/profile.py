"""
    Sample some process statistics from /proc/[pid]/status
     
    polls these fields from /proc/pid/stat:
    see "man proc" for more info, or see read_man_proc.py
    minflt, cminflt, majflt, utime, stime, cutime, cstime, priority, nice, num_threads, exit_signal, delayacct_blkio_ticks
    *removed rsslim for now*

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
import subprocess,time,sys,re,os,sqlite3,json
import read_man_proc

class Profile:
    fields_to_get = {'UPDATE': ['VmPeak','VmHWM'] + #/proc/status
                               ['minflt','cminflt','majflt','utime','stime','cutime','cstime','delayacct_blkio_ticks'], #/proc/stat
                     'INSERT': ['FDSize','VmSize','VmLck','VmRSS','VmData','VmLib','VmPTE'] + #/proc/status
                               ['num_threads'] #/proc/stat
                     }
    
    proc = None #the main subprocess object
    poll_number = 0 #number of polls
    
    @property
    def all_pids(self):
        """This main process and all of its descendant's pids"""
        return self.and_descendants(os.getpid())
    
    def __init__(self,command,poll_interval=1):
        self.command = command
        self.poll_interval = poll_interval
        self.conn = sqlite3.connect(':memory:')
        self.c = self.conn.cursor()
        #Create Records Table
        insert_fields = self.fields_to_get['INSERT']
        sqfields = ', '.join(map(lambda x: x + ' INTEGER', insert_fields))
        self.c.execute("CREATE TABLE record (pid INTEGER, poll_number INTEGER, {0})".format(sqfields))
        #Create Processes Table
        update_fields = self.fields_to_get['UPDATE']
        sqfields = ', '.join(map(lambda x: x + ' INTEGER', update_fields))
        self.c.execute("CREATE TABLE process (pid INTEGER PRIMARY KEY, poll_number INTEGER, name TEXT, {0})".format(sqfields))
        
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
            return [pid] + self._unnest([ self.and_descendants(int(child)) for child in children ])
    
    
    def run(self):
        """Runs a process and records the memory usage of it and all of its descendants"""
        self.proc = subprocess.Popen(self.command, shell=True)
        while True:
            self.poll_all_procs()
            time.sleep(1)
            if self.proc.poll() != None:
                self.finish()
    
    def parseVal(self,val):
        "Remove kB and return ints."
        return int(val) if val[-2:] != 'kB' else int(val[0:-3])
        
    def poll_all_procs(self):
        """Updates the sql table with all descendant processes' resource usage"""
        self.poll_number = self.poll_number + 1
        for pid in self.all_pids:
            try:
                all_stats = self.read_proc_stat(pid)
                all_stats += self.read_proc_status(pid)
                #Inserts
                inserts = [ (name,self.parseVal(val)) for name,val in all_stats if name in self.fields_to_get['INSERT'] ] + [('pid',pid),('poll_number',self.poll_number)]
                keys,vals = zip(*inserts) #unzip
                q = "INSERT INTO record ({keys}) values({s})".format(s = ','.join(['?']*len(vals)),
                                                                     keys = ', '.join(keys))
                self.c.execute(q,vals)
                #Updates
                proc_name = filter(lambda x: x[0]=='Name' ,all_stats)[0][1]
                updates = [ (name,self.parseVal(val)) for name,val in all_stats if name in self.fields_to_get['UPDATE'] ] + [('pid',pid),('Name',proc_name),('poll_number',self.poll_number)]
                keys,vals = zip(*updates) #unzip
                q = "INSERT OR REPLACE INTO process ({keys}) values({s})".format(s = ','.join(['?']*len(vals)),
                                                                     keys = ', '.join(keys))
                print q
                print vals
                self.c.execute(q,vals)
                
            except IOError:
                pass # job finished
                 
        
    def read_proc_stat(self,pid):
        """
        :returns: (field_name,value) from /proc/pid/stat or None if its empty
        """
        stat_fields = read_man_proc.get_stat_and_status_fields()
        with open('/proc/{0}/stat'.format(pid),'r') as f:
            stat_all = f.readline().split(' ')
            return map(lambda x: (x[0][0],x[1]),zip(stat_fields,stat_all))
        
    def read_proc_status(self,pid):
        """
        :returns: (field_name,value) from /proc/pid/status or None if its empty
        """
        def line2tuple(l):
            m = re.search(r"\s*(.+):\s*(.+)\s*",l)
            return m.group(1),m.group(2)
        with open('/proc/{0}/status'.format(pid),'r') as f:
            return map(line2tuple,f.readlines())
        
        
    def analyze_records(self):
        import pprint
        #descendant process summaries
        self.c.execute("""
            SELECT * FROM
            (
            SELECT pid, 
            MAX(FDSize), AVG(FDSize), 
            AVG(VmSize), 
            MAX(VmLck), AVG(VmLck), 
            AVG(VmRSS), 
            MAX(VmData), AVG(VmData), 
            MAX(VmLib), AVG(VmLib), 
            MAX(VmPTE), AVG(VmPTE),
            MAX(num_threads), AVG(num_threads) 
            FROM record
            GROUP BY pid ) as foo
            JOIN
            (SELECT * from process) as bar
            on foo.pid=bar.pid
            """)
        keys = [ x[0] for x in self.c.description]
        pprint.pprint( [ dict(zip(keys,vals)) for vals in self.c ] )
        
        #Combined summary
        self.c.execute("""
            (SELECT pid, 
            MAX(FDSize), AVG(FDSize), 
            AVG(VmSize), 
            MAX(VmLck), AVG(VmLck), 
            AVG(VmRSS), 
            MAX(VmData), AVG(VmData), 
            MAX(VmLib), AVG(VmLib), 
            MAX(VmPTE), AVG(VmPTE),
            MAX(num_threads), AVG(num_threads) 
            FROM
                (
                SELECT pid, 
                SUM(FDSize),
                SUM(VmSize), 
                SUM(VmLck), 
                SUM(VmLck), 
                SUM(VmData),
                SUM(VmLib), 
                SUM(VmPTE),
                SUM(num_threads)
                FROM record
                GROUP BY pid,poll_number )
            ) as foo
            JOIN
            (SELECT * from process) as bar
            on foo.pid=bar.pid
            
                """)
        keys = [ x[0] for x in self.c.description]
        pprint.pprint( [ dict(zip(keys,vals)) for vals in self.c ] )
        
        
    
    def finish(self):
        self.analyze_records()
        sys.exit(self.proc.poll())

if __name__ == '__main__':
    profile = Profile('wc -l ~/Downloads/Revolution.2012.S01E01.720p.HDTV.X264-DIMENSION.mkv')
    profile = Profile('sleep 3')
    profile.run()