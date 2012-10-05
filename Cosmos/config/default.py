import os

#Cosmos Settings
home_path = '/home2/erik/workspace/Cosmos' #bioseq
default_root_output_dir = '/mnt' #The root directory to output workflows to
DRM = 'GE' # 'LSF' or 'GE'
time_path = '/usr/bin/time'

#starcluster
DATABASE = {
    'ENGINE': 'django.db.backends.mysql', # Add 'postgresql_psycopg2', 'postgresql', 'mysql', 'sqlite3' or 'oracle'.
    'NAME': 'pype',                      # Or path to database file if using sqlite3.
    'USER': 'cosmos',                      # Not used with sqlite3.
    'PASSWORD': '9LxMpadB95q4efHB',                  # Not used with sqlite3.
    'HOST': '',                      # Set to empty string for localhost. Not used with sqlite3.
    'PORT': '',                      # Set to empty string for default. Not used with sqlite3.
}


###SGE
#os.environ['DRMAA_LIBRARY_PATH'] = '/opt/sge6/lib/linux-x64/libdrmaa.so'
#os.environ['SGE_ROOT'] = '/opt/sge6'
#os.environ['SGE_EXECD_PORT'] = '63232'
#os.environ['SGE_QMASTER_PORT'] = '63231'

###LSF
#os.environ['LSF_DRMAA_CONF']='/opt/lsf/conf/lsf_drmaa.conf'
#os.environ['DRMAA_LIBRARY_PATH']='/opt/lsf/7.0/linux2.6-glibc2.3-x86_64/lib/libdrmaa.so'