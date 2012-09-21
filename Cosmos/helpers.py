import re
from django.core.exceptions import ValidationError
import os
import logging

def parse_command_string(txt,**kwargs):
    """removes empty lines and white space.
    also .format()s with the **kwargs dictioanry"""
    x = txt.split('\n')
    x = map(lambda x: x.strip(),x)
    x = filter(lambda x: not x == '',x)
    try:
        s = '\n'.join(x).format(**kwargs)
    except KeyError:
        logging.warning('*'*76)
        logging.warning("format() error occurred here:")
        logging.warning('txt is:')
        logging.warning('\n'.join(x))
        logging.warning('-'*76)
        logging.warning('keys available are:')
        logging.warning(kwargs.keys())
        logging.warning('*'*76)
        raise ValidationError("Format() KeyError.  You did not pass the proper arguments to format() the txt.")
    return s


def validate_name(txt,field_name=''):
    """
    Validates that txt is alphanumeric and underscores, decimals, or hyphens only
    """
    if re.match('^[a-zA-Z0-9_\.\s-]+$',txt) == None:
        raise ValidationError('Field {} must be alphanumeric and underscores, periods, spaces, or hyphens only.  Text that failed: {}'.format(field_name,txt))
    
def validate_not_null(field):
    if field == None:
        raise ValidationError('Required field left blank')
    
def check_and_create_output_dir(path):
    """
    checks if a path exists and whether its valid.  If it does not exist, create it
    """
    if os.path.exists(path):
        if not os.path.isdir(path):
            raise ValidationError('Path is not a directory')
    else:
        os.mkdir(path)
    
def addExt(file_path,new_extension,remove_dir_path=True):
    """
    Adds an extension to a filename
    remove_dir_path will remove the directory path and return only the filename with the added extension
    """
    if remove_dir_path:
        dir,file_path = os.path.split(file_path)
    return re.sub(r'^(.*)(\..+)$', r'\1.{}\2'.format(new_extension), file_path)

def sizeof_fmt(num):
    for x in ['bytes','KB','MB','GB']:
        if num < 1024.0 and num > -1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0
    return "%3.1f %s" % (num, 'TB')

def folder_size(folder):
    folder_size = 0
    for (path, dirs, files) in os.walk(folder):
        for f in files:
            filename = os.path.join(path, f)
            folder_size += os.path.getsize(filename)
            
    return sizeof_fmt(folder_size)

def get_logger(name,path):
    """
    Gets a logger of name `name` that prints to stderr and to path
    """
    log = logging.getLogger(name)
    #logging.basicConfig(level=logging.DEBUG)
    
    #check if we've already configured logger
    if len(log.handlers) > 0:
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
    """
    log_dir = os.path.join(workflow.output_dir,'log')
    path = os.path.join(log_dir,'main.log')
    check_and_create_output_dir(log_dir)
    return (get_logger(workflow.name,path), path)
    

