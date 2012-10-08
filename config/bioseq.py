import os

#Cosmos Settings
home_path = '/home2/erik/workspace/Cosmos' #bioseq
default_root_output_dir = '/mnt/cosmos_out'
DRM = 'GE'
tmp_dir = '/mnt/tmp'

#starcluster
DATABASE = {
    'ENGINE': 'django.db.backends.mysql', # Add 'postgresql_psycopg2', 'postgresql', 'mysql', 'sqlite3' or 'oracle'.
    'NAME': 'pype',                      # Or path to database file if using sqlite3.
    'USER': 'cosmos',                      # Not used with sqlite3.
    'PASSWORD': '9LxMpadB95q4efHB',                  # Not used with sqlite3.
    'HOST': '',                      # Set to empty string for localhost. Not used with sqlite3.
    'PORT': '',                      # Set to empty string for default. Not used with sqlite3.
}
#
#
####SGE
#os.environ['DRMAA_LIBRARY_PATH'] = '/opt/sge6/lib/linux-x64/libdrmaa.so'
#os.environ['SGE_ROOT'] = '/opt/sge6'
#os.environ['SGE_EXECD_PORT'] = '63232'
#os.environ['SGE_QMASTER_PORT'] = '63231'
