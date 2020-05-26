#!/usr/bin/python3

import setuptools
   
setuptools.setup(
    name='common setup.py',
    version='1.0',
    description='Generic setup.py to allow roll up of dependencies for any dataflow job',
    maintainer='null',
    maintainer_email='null',
    url='null',
    install_requires=[], # include any python modules that don't exist on dataflow vms and need to be pip installed here
    packages=setuptools.find_packages(),
 )
