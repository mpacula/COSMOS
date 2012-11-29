import os

# Cosmos Settings
home_path = '/home/ch158749/workspace/Cosmos' #gpp
default_root_output_dir = '/nas/erik/cosmos_output3' #gpp
DRM = 'LSF' #LSF, or GE
tmp_dir = '/nas/erik/tmp'

# Web interface settings
show_stage_file_sizes = False # on some systems, file i/o slows down a lot when running a lot of jobs making this feature very slow
show_jobAttempt_file_sizes = False
show_task_file_sizes = False

#DATABASE = {
#    'ENGINE': 'django.db.backends.mysql', # Add 'postgresql_psycopg2', 'postgresql', 'mysql', 'sqlite3' or 'oracle'.
#    'NAME': 'Cosmos',                      # Or path to database file if using sqlite3.
#    'USER': 'cosmos',                      # Not used with sqlite3.
#    'PASSWORD': '12345erik',                  # Not used with sqlite3.
#    'HOST': 'GP-DB.tch.harvard.edu',                      # Set to empty string for localhost. Not used with sqlite3.
#    'PORT': '',                      # Set to empty string for default. Not used with sqlite3.
#}


DATABASE = {
    'ENGINE': 'django.db.backends.sqlite3', # Add 'postgresql_psycopg2', 'postgresql', 'mysql', 'sqlite3' or 'oracle'.
    'NAME': '/home/ch158749/cosmos.sqlite',                      # Or path to database file if using sqlite3.
    'USER': '',                      # Not used with sqlite3.
    'PASSWORD': '',                  # Not used with sqlite3.
    'HOST': '',                      # Set to empty string for localhost. Not used with sqlite3.
    'PORT': '',                      # Set to empty string for default. Not used with sqlite3.
}

