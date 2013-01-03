import os

#Cosmos Settings
home_path = '/home2/erik/workspace/Cosmos' #bioseq
default_root_output_dir = '/cosmos/output'
DRM = 'GE'
tmp_dir = '/mnt/tmp'
default_queue = None

#starcluster
DATABASE = {
    'ENGINE': 'django.db.backends.mysql', # Add 'postgresql_psycopg2', 'postgresql', 'mysql', 'sqlite3' or 'oracle'.
    'NAME': 'pype',                      # Or path to database file if using sqlite3.
    'USER': 'cosmos',                      # Not used with sqlite3.
    'PASSWORD': '9LxMpadB95q4efHB',                  # Not used with sqlite3.
    'HOST': '',                      # Set to empty string for localhost. Not used with sqlite3.
    'PORT': '',                      # Set to empty string for default. Not used with sqlite3.
}

### SGE Specific
parallel_environment_name = 'orte' #the name of the SGE parallel environment name.  Use "qconf -spl" to list available names