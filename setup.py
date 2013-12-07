from distutils.core import setup
from setuptools import find_packages
import os
import sys
from subprocess import Popen,PIPE

from cosmos import __version__

README = open('README.rst').read()

def all_files(path,num_root_dir_to_skip=1):
    all= map(lambda x: x.strip(),Popen(['find',path],stdout=PIPE).stdout.readlines())
    return map(lambda x: '/'.join(x.split('/')[num_root_dir_to_skip:]), filter(os.path.isfile,all))

examples_installation_dir = os.path.expanduser('~/.cosmos/example_workflows')
print >> sys.stderr, "Installing userfiles to ~/.cosmos"

example_workflows = map(lambda x:os.path.join('example_workflows/',x),filter(lambda x:x[-3:]=='.py',os.listdir('example_workflows')))

setup(name='cosmos',
    version=__version__,
    description = "Workflow Manager",
    author='Erik Gafni',
    license='Non-commercial',
    long_description=README,
    packages=find_packages(),
    scripts=['bin/cosmos'],
    package_data={'cosmos':['default_config.ini','lsf_drmaa.conf']+all_files('cosmos/static')+all_files('cosmos/templates')},
    data_files=[(examples_installation_dir,example_workflows)],
    install_requires=[
        'distribute>=0.6.28',
        'Django==1.6',
        'configobj==4.7.2',
        'decorator==3.4.0',
        'django-extensions',
        'django-picklefield==0.3.1',
        'drmaa==0.5',
        'ipython',
        'networkx',
        'pygraphviz',
        'ordereddict',
        'pyparsing',
        'pydot'
        'sphinx-rtd-theme'
    ]
)