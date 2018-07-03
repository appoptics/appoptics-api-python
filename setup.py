#!/usr/bin/python
#
import os
import sys
from setuptools import setup

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()

setup(
    name="appoptics-metrics",
    version="5.0.0",        # Update also in __init__ ; look into zest.releaser to avoid having two versions
    description="Python API Wrapper for AppOptics Metrics",
    long_description="Python Wrapper for the AppOptics Metrics API: https://docs.appoptics.com/kb/custom_metrics/api",
    author="AppOptics",
    author_email="support@appoptics.com",
    url='https://github.com/appoptics/appoptics-api-python',
    license='https://github.com/appoptics/appoptics-api-python/blob/master/LICENSE',
    packages=['appoptics_metrics'],
    package_data={'': ['LICENSE', 'README.md', 'CHANGELOG.md']},
    package_dir={'appoptics_metrics': 'appoptics_metrics'},
    include_package_data=True,
    platforms='Posix; MacOS X; Windows',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Topic :: Internet',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
    ],
    dependency_links=[],
    install_requires=['six'],
)
