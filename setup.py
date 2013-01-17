from distutils.core import setup
from setuptools import find_packages

README = open('README.rst').read()

setup(name='cosmos',
    version='0.2',
    description = "Workflow Manager",
    author='Erik Gafni',
    license='Non-commercial',
    long_description=README,
    packages=find_packages(),
    scripts=['bin/cosmos','bin/manage'],
    data_files={},
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