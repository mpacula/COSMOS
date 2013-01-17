import os

#Cosmos Settings
home_path = '/home/esg21/workspace/Cosmos' #orchestra
default_root_output_dir = '/groups/cbmi/erik/cosmos_out' #orchestra
default_root_output_dir = '/scratch/esg21/cosmos_out' #orchestra
DRM = 'LSF'
tmp_dir='/scratch/esg21/tmp'
tmp_dir='/groups/lpm/erik/cosmos/tmp'
default_queue = "i2b2_unlimited"

DATABASE = {
    'ENGINE': 'django.db.backends.mysql', # Add 'postgresql_psycopg2', 'postgresql', 'mysql', 'sqlite3' or 'oracle'.
    'NAME': 'cosmos',                      # Or path to database file if using sqlite3.
    'USER': 'esg21',                      # Not used with sqlite3.
    'PASSWORD': 'nWTTpHS7RdtEyL8K',                  # Not used with sqlite3.
    'HOST': 'mysql.orchestra',                      # Set to empty string for localhost. Not used with sqlite3.
    'PORT': '',                      # Set to empty string for default. Not used with sqlite3e
    }


########################
# Web interface
########################

# on some systems, file i/o slows down a lot when running a lot of jobs making file_size calculations very slow.
show_stage_file_sizes = True
show_jobAttempt_file_sizes = True
show_task_file_sizes = True
