from distutils.core import setup
from setuptools import find_packages
import os
import os
from subprocess import Popen,PIPE

README = open('README.rst').read()

def all_files(path):
    all= map(lambda x: x.strip(),Popen(['find',path],stdout=PIPE).stdout.readlines())
    return map(lambda x: '/'.join(x.split('/')[1:]), filter(os.path.isfile,all))

setup(name='cosmos',
    version='0.2',
    description = "Workflow Manager",
    author='Erik Gafni',
    license='Non-commercial',
    long_description=README,
    packages=find_packages(),
    scripts=['bin/cosmos'],
    package_data={'cosmos':['default_config.ini']+all_files('cosmos/Cosmos/static')+all_files('cosmos/templates')},
    install_requires=['decorator',
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