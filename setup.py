from distutils.core import setup
from setuptools import find_packages
import os
import os

def all_files(path):
    for tuple in os.walk(path):
        dir = tuple[0]
        files = tuple[1]
        for f in files:
            p= os.path.join(dir,f)
            print p
            if os.path.isfile(p):
                yield p

static = list(all_files('cosmos/Cosmos/static'))
print static

README = open('README.rst').read()

setup(name='cosmos',
    version='0.2',
    description = "Workflow Manager",
    author='Erik Gafni',
    license='Non-commercial',
    long_description=README,
    packages=find_packages(),
    scripts=['bin/cosmos'],
    package_data={'cosmos':['config.ini']+['cosmos/*.*']},
    install_requires=['decorator',
                      'Django',
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