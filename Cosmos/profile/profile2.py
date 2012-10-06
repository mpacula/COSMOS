import subprocess,resource,time,os,signal
#
#p=subprocess.Popen('sleep 5',shell=True)
#print resource.getrusage(resource.RUSAGE_SELF)
#time.sleep(1)
#print resource.getrusage(resource.RUSAGE_SELF)

def user_ctrl_c(s, f):
        print 'caught ctrl+c'

signal.signal(signal.SIGINT, user_ctrl_c)
time.sleep(10)