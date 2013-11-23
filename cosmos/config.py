import os
from configobj import ConfigObj

from cosmos import django_settings
from django.conf import settings as django_conf_settings, global_settings

cosmos_library_path = os.path.dirname(os.path.realpath(__file__))
default_config_path = os.path.join(cosmos_library_path, 'default_config.ini')

user_home_path = os.path.expanduser('~')
cosmos_path = os.path.join(user_home_path, '.cosmos/')
config_path = os.path.join(cosmos_path, 'config.ini')
settings = None

def configure(
        default_root_output_dir,
        database,
        working_directory='/tmp',
        license_warning=False,
        DRM='local',
        default_queue=None,
        drmaa_library_path='/path/to/drmaa.so',
        show_stage_file_sizes=True,
        show_jobAttempt_file_sizes=True,
        show_task_file_sizes=True,
        show_stage_details=True,
        auto_refresh_workflows=True,
        server_name=None,
        django_settings_dict=None,
        **kwargs
):
    """
    Manually configure Cosmos.  Alternatively, Cosmos will automatically configure itself by using ~/.cosmos/config.ini.
    Will only configure django if it hasn't been configured yet.

    :param default_root_output_dir: The directory to write all workflow output files to
    :param database: (dict) a dict with keywords:
        * ENGINE     set to postgresql_psycopg2, postgresql, mysql, sqlite3 or oracle.
        * NAME       Or path to database file if using sqlite3.
        * USER       Note not used with sqlite3.
        * PASSWORD   Note Not used with sqlite3.
        * HOST       Set to empty string for localhost. Not used with sqlite3.
        * PORT       Set to empty string for default. Not used with sqlite3.
    :param working_directory: The working directory for submitted jobs.
    :param license_warning: Print license warning to console when using Cosmos
    :param DRM: The DRM module you'd like Cosmos to use.  Options are DRMAA_LSF, DRMAA_GE, Native_LSF, local.
    :param default_queue: Default queue name to submit jobs to
    :param drmaa_library_path: The path to the drmaa.so file.  Highly system dependent, but common locations include:
        /opt/sge6/lib/linux-x64/libdrmaa.so
        /opt/lsf/7.0/linux2.6-glibc2.3-x86_64/lib/libdrmaa.so
    :param django_settings: A dict to call django's settings.configure() with.
      If None, default is to use cosmos.django_settings.gen_config
    :return:
    """
    global settings
    d = locals()
    d.update(d.pop('kwargs'))
    d['cosmos_library_path'] = os.path.dirname(os.path.realpath(__file__))

    if django_settings_dict is None:
        django_settings_dict = django_settings.gen_config(cosmos_library_path, d['database'])
        d['django_settings_dict'] = django_settings_dict

    if not django_conf_settings.configured and django_settings_dict is not None:
        django_conf_settings.configure(
            #custom template context processor for web interface
            TEMPLATE_CONTEXT_PROCESSORS=global_settings.TEMPLATE_CONTEXT_PROCESSORS + (
            'cosmos.utils.context_processor.contproc',),
            #load django settings
            **django_settings_dict)

    settings = d

def read_cosmos_config(config_path=config_path):
    if not os.path.exists(config_path):
        raise OSError, '{0} does not exit'.format(config_path)

    # Creating settings dictionary
    co = ConfigObj(config_path)
    d = co.dict()
    d['database'] = {'default': d.pop('Database')}
    return d


def configure_from_file(config_path=config_path):
    d = read_cosmos_config(config_path=config_path)
    configure(**d)
