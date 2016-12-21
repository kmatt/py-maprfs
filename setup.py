#!/usr/bin/env python

import os
from setuptools import setup

setup(
    name='maprfs',
    version='0.1.2',
    description='Python wrappers for MapRFS',
    url='https://github.com/IDAnalytics/py-maprfs/',
    maintainer='Daniel Grady',
    maintainer_email='dgrady@idanalytics.com',
    license='BSD',
    keywords='hdfs mapr',
    packages=['maprfs'],
    install_requires=[],
    long_description=(open('README.rst').read() if os.path.exists('README.rst')
                      else ''),
    zip_safe=False
)
