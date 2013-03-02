from cosmos.contrib.ezflow.tool import Tool
import os

class Bunzip2(Tool):
    name = "Bunzip"
    mem_req = 4*1024
    cpu_req = 2
    time_req = 100
    inputs = ['bz2']
    outputs = ['*']
    default_params = { 'q': 5 }

    def cmd(self,i,s,p):
        """
        Expects tags: lane, chunk, library, sample, platform, flowcell, pair
        """
        return 'bunzip2 -c {i[bz2]} > $OUT.*'
