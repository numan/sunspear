#!/usr/bin/python

from setuptools import setup, find_packages

tests_require=[
    'nose',
    'mock',
]

setup(
    name="sunspear",
    license='Apache License 2.0',
    version="0.3.3",
    description="Activity streams backed by Riak.",
    zip_safe=False,
    long_description=open('README.rst', 'r').read(),
    author="Numan Sachwani",
    author_email="numan856@gmail.com",
    url="https://github.com/numan/sunspear",
    packages=find_packages(exclude=['tests']),
    test_suite='nose.collector',
    install_requires=[
        'riak==2.5.4',
        'python-dateutil>=1.5, != 2.0',
        'protobuf==2.6.1',
        'sqlalchemy==1.1.14',
        'MySQL-python==1.2.5',
        'six'
    ],
    options={'easy_install': {'allow_hosts': 'pypi.python.org'}},
    tests_require=tests_require,
    extras_require={"test": tests_require, "nosetests": tests_require},
    include_package_data=True,
    classifiers=[
        "Intended Audience :: Developers",
        'Intended Audience :: System Administrators',
        "Programming Language :: Python",
        "Topic :: Software Development",
        "Topic :: Utilities",
    ],
)
