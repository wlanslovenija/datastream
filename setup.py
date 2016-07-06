#!/usr/bin/env python

import os

from setuptools import setup, find_packages

VERSION = '0.5.8'

if __name__ == '__main__':
    setup(
        name='datastream',
        version=VERSION,
        description="Datastream API time-series library.",
        long_description=open(os.path.join(os.path.dirname(__file__), 'README.rst')).read(),
        author='wlan slovenija',
        author_email='open@wlan-si.net',
        url='https://github.com/wlanslovenija/datastream',
        license='AGPLv3',
        packages=find_packages(exclude=('*.tests', '*.tests.*', 'tests.*', 'tests')),
        package_data={},
        classifiers=[
            'Development Status :: 4 - Beta',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: GNU Affero General Public License v3',
            'Operating System :: OS Independent',
            'Programming Language :: Python',
        ],
        include_package_data=True,
        zip_safe=False,
        install_requires=[
            'mongoengine>=0.8.1',
            'pymongo>=2.7.1,<3.0.0',
            'pytz>=2012h',
            'python-dateutil>=2.4.2',
            'psycopg2>=2.6.1',
            'influxdb>=2.10.0',
            'backports.lzma>=0.0.6',
        ],
        tests_require=[
            'mongoengine>=0.8.1',
            'pymongo>=2.7.1,<3.0.0',
            'pytz>=2012h',
            'python-dateutil>=2.4.2',
            'psycopg2>=2.6.1',
            'influxdb>=2.10.0',
        ],
        test_suite='tests',
    )
