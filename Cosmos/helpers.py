import re
from django.core.exceptions import ValidationError
import os
import logging

from django.db import transaction

@transaction.commit_manually
def flush_transaction():
    """
    Flush the current transaction so we don't read stale data

    Use in long running processes to make sure fresh data is read from
    the database.  This is a problem with MySQL and the default
    transaction mode.  You can fix it by setting
    "transaction-isolation = READ-COMMITTED" in my.cnf or by calling
    this function at the appropriate moment
    """
    transaction.commit()



def validate_name(txt,field_name=''):
    """
    Validates that txt is alphanumeric and underscores, decimals, or hyphens only
    """
    if re.match('^[a-zA-Z0-9_\.-]+$',txt) == None:
        raise ValidationError('Field {} must be alphanumeric and underscores, decimals, or hyphens only.  Text that failed: {}'.format(field_name,txt))
    
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
    
    #check if we've already configured logger
    if len(filter(lambda x: x.name=='cosmos_fh',log.handlers)) > 0:
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
    Gets a logger of name `name` that prints to stderr and to workflow/log/main.log
    """
    log_dir = os.path.join(workflow.output_dir,'log')
    path = os.path.join(log_dir,'main.log')
    check_and_create_output_dir(log_dir)
    return (get_logger(workflow.name,path), path)
    

