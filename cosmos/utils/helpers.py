import re
import os
import logging
import subprocess
import itertools
import pprint
import sys
import signal

class ValidationException(Exception): pass

real_stdout = os.dup(1)
real_stderr = os.dup(2)
#devnull = os.open('/tmp/erik_drmaa_garbage', os.O_WRONLY)
devnull = os.open('/dev/null', os.O_WRONLY)
def disable_stderr():
    sys.stderr.flush()
    os.dup2(devnull,2)
def enable_stderr():
    sys.stderr.flush()
    os.dup2(real_stderr,2)
def disable_stdout():
    sys.stderr.flush()
    os.dup2(devnull,1)
def enable_stdout():
    sys.stderr.flush()
    os.dup2(real_stdout,1)


def representsInt(s):
    """
    :param s: A string
    :return: True of `s` can be converted to an int, otherwise False
    """
    try:
        int(s)
        return True
    except ValueError:
        return False

def confirm(prompt=None, default=False, timeout=0):
    """prompts for yes or no defaultonse from the user. Returns True for yes and
    False for no.

    'default' should be set to the default value assumed by the caller when
    user simply types ENTER.

    :param timeout: (int) If set, prompt will return default.

    >>> confirm(prompt='Create Directory?', default=True)
    Create Directory? [y]|n: 
    True
    >>> confirm(prompt='Create Directory?', default=False)
    Create Directory? [n]|y: 
    False
    >>> confirm(prompt='Create Directory?', default=False)
    Create Directory? [n]|y: y
    True
    """
    class TimeOutException(Exception): pass
    def timed_out(signum, frame):
        "called when stdin read times out"
        raise TimeOutException('Timed out')
    signal.signal(signal.SIGALRM, timed_out)

    if prompt is None:
        prompt = 'Confirm'

    if default:
        prompt = '%s [%s]|%s: ' % (prompt, 'y', 'n')
    else:
        prompt = '%s [%s]|%s: ' % (prompt, 'n', 'y')

    while True:
        signal.alarm(timeout)
        try:
            ans = raw_input(prompt)
            signal.alarm(0)
            if not ans:
                return default
            if ans not in ['y', 'Y', 'yes', 'n', 'no', 'N']:
                print 'please enter y or n.'
                continue
            if ans in ['y','yes','Yes']:
                return True
            if ans in ['n','no','N']:
                return False
        except TimeOutException:
            print "Confirmation timed out after {0}s, returning default of '{1}'".format(timeout,'yes' if default else 'no')
            return default


def formatError(txt,dict):
    """
    Prints a useful debugging message for a bad .format() call, then raises an exception
    """
    logging.warning('*'*76)
    logging.warning("format() error occurred here:")
    logging.warning('txt is:\n'+txt)
    logging.warning('-'*76)
    logging.warning('dict available is:\n'+pprint.pformat(dict,indent=4))
    
    logging.warning('*'*76)
    raise Exception("Format() KeyError.  You did not pass the proper arguments to format() the txt.")
    

def groupby(iterable,fxn):
    """aggregates an iterable using a function"""
    return itertools.groupby(sorted(iterable,key=fxn),fxn)

def parse_cmd(txt,**kwargs):
    """removes empty lines and white spaces, and appends a \ to the end of every line.
    also .format()s with the **kwargs dictioanry"""
    try:
        x = txt.format(**kwargs)
        x = x.split('\n')
        x = map(lambda x: re.sub(r"\\$",'',x.strip()).strip(),x)
        x = filter(lambda x: not x == '',x)
        x = ' \\\n'.join(x)
    except (KeyError,TypeError):
        formatError(txt,kwargs)
    return x


def spinning_cursor(i):
    ":reutrn: a string that represents part of a spinning cursor"
    cursor='/-\|'
    while 1:
        return cursor[i % len(cursor)]


def validate_name(txt,field_name=''):
    """
    Validates that txt is alphanumeric and underscores, decimals, or hyphens only
    """ 
    if re.match('^[a-zA-Z0-9_\.\s-]+$',txt) == None:
        raise ValidationException('Field {0} must be alphanumeric, periods, spaces, or hyphens only.  Text that failed: {1}'.format(field_name,txt))
    
def validate_not_null(field):
    if field == None:
        raise ValidationException('Required field left blank')
    
def check_and_create_output_dir(path):
    """
    checks if a path exists and whether its valid.  If it does not exist, create it
    """
    if os.path.exists(path):
        if not os.path.isdir(path):
            raise ValidationException('Path is not a directory')
    else:
        os.system('mkdir -p {0}'.format(path))
        #os.mkdir(path)

def execute(cmd):
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    c = p.communicate()
    return c[0],c[1]

def folder_size(folder,human_readable=True):
    if human_readable:
        return re.match('(.+?)\s',execute('du -hs {0}'.format(folder))[0]).group(1)
    else:
        return re.match('(.+?)\s',execute('du -s {0}'.format(folder))[0]).group(1)

def get_logger(name,path):
    """
    Gets a logger of name `name` that prints to stderr and to path
    """
    log = logging.getLogger(name)
    #logging.basicConfig(level=logging.DEBUG)
    
    #check if we've already configured logger
    if len(log.handlers) > 1:
        return log
    
    log.setLevel(logging.INFO)
    # create file handler which logs even debug messages
    fh = logging.FileHandler(path)
    fh.setLevel(logging.INFO)
    #fh.set_name('cosmos_fh')
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    #ch.set_name('cosmos_fh')
    # add the handlers to logger
    fh.setFormatter(logging.Formatter('%(levelname)s: %(asctime)s: %(message)s',"%Y-%m-%d %H:%M:%S"))
    ch.setFormatter(logging.Formatter('%(levelname)s: %(asctime)s: %(message)s',"%Y-%m-%d %H:%M:%S"))
    log.addHandler(ch)
    log.addHandler(fh)
    return log
    

def get_workflow_logger(workflow):
    """
    Returns a logger configured for a Workflow
    """
    log_dir = os.path.join(workflow.output_dir,'log')
    path = os.path.join(log_dir,'main.log')
    if os.path.exists(path):
        check_and_create_output_dir(log_dir)
        return (get_logger(workflow.name,path), path)
    else:
        return None,None
    

