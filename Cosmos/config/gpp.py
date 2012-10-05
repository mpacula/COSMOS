import os

# Cosmos Settings
home_path = '/home/ch158749/workspace/Cosmos' #gpp
default_root_output_dir = '/nas/erik/cosmos_output' #gpp
DRM = 'LSF' #LSF, or GE
tmp_dir = '/nas/erik/tmp'
time_path = '/usr/bin/time'

# Web interface settings
show_batch_file_sizes = False # on some systems, file i/o slows down a lot when running a lot of jobs making this feature very slow

DATABASE = {
    'ENGINE': 'django.db.backends.mysql', # Add 'postgresql_psycopg2', 'postgresql', 'mysql', 'sqlite3' or 'oracle'.
    'NAME': 'Cosmos',                      # Or path to database file if using sqlite3.
    'USER': 'cosmos',                      # Not used with sqlite3.
    'PASSWORD': '12345erik',                  # Not used with sqlite3.
    'HOST': 'GP-DB.tch.harvard.edu',                      # Set to empty string for localhost. Not used with sqlite3.
    'PORT': '',                      # Set to empty string for default. Not used with sqlite3.
}


###LSF
os.environ['LSF_DRMAA_CONF']='/home/ch158749/lsf_drmaa.conf'
os.environ['DRMAA_LIBRARY_PATH']='/opt/lsf/7.0/linux2.6-glibc2.3-x86_64/lib/libdrmaa.so'

