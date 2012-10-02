import os

#Cosmos Settings
home_path = '/home/esg21/workspace/Cosmos' #orchestra
default_root_output_dir = '/scratch/esg21' #orchestra
DRM = 'LSF'
time_path = '/usr/bin/time'


DATABASE = {
    'ENGINE': 'django.db.backends.sqlite3', # Add 'postgresql_psycopg2', 'postgresql', 'mysql', 'sqlite3' or 'oracle'.
    'NAME': '/home/esg21/workspace/Cosmos/sqlite.db',                      # Or path to database file if using sqlite3.
    'USER': '',                      # Not used with sqlite3.
    'PASSWORD': '',                  # Not used with sqlite3.
    'HOST': '',                      # Set to empty string for localhost. Not used with sqlite3.
    'PORT': '',                      # Set to empty string for default. Not used with sqlite3.
}


###LSF
os.environ['LSF_DRMAA_CONF']='/opt/lsf/conf/lsf_drmaa.conf'
os.environ['DRMAA_LIBRARY_PATH']='/opt/lsf/7.0/linux2.6-glibc2.3-x86_64/lib/libdrmaa.so'