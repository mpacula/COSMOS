from distutils.core import setup
from setuptools import find_packages
import os
import sys
from subprocess import Popen,PIPE

README = open('README.rst').read()

def all_files(path,num_root_dir_to_skip=1):
    all= map(lambda x: x.strip(),Popen(['find',path],stdout=PIPE).stdout.readlines())
    return map(lambda x: '/'.join(x.split('/')[num_root_dir_to_skip:]), filter(os.path.isfile,all))

examples_installation_dir = os.path.expanduser('~/.cosmos/example_workflows')
print >> sys.stderr, "Installing userfiles to ~/.cosmos"

example_workflows = map(lambda x:os.path.join('example_workflows/',x),filter(lambda x:x[-3:]=='.py',os.listdir('example_workflows')))

setup(name='cosmos',
    version='0.3',
    description = "Workflow Manager",
    author='Erik Gafni',
    license='Non-commercial',
    long_description=README,
    packages=find_packages(),
    scripts=['bin/cosmos'],
    package_data={'cosmos':['default_config.ini']+all_files('cosmos/static')+all_files('cosmos/templates')},
    data_files=['README.rst','LICENSE']+[(examples_installation_dir,example_workflows)],
    install_requires=[
        'distribute>=0.6.28',
        'decorator',
        'Django',
        'configobj',
        'MySQL-python',
        'networkx',
        'django-extensions',
        'django-picklefield',
        'drmaa',

        #optional
        'pygraphviz',
        'south',
        'ipython',
    ]
)