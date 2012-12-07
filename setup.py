#!/usr/bin/python

from setuptools import setup, find_packages

setup(
    name="sunspear",
    license='Apache License 2.0',
    version="0.1.0-ALPHA",
    description="Activity streams backed by Riak.",
    long_description=open('README.rst', 'r').read(),
    author="Numan Sachwani",
    author_email="numan856@gmail.com",
    url="https://github.com/numan/sunspear",
    packages=find_packages(exclude=['tests']),
    test_suite='nose.collector',
    install_requires=[
        'riak==1.5.1',
        'python-dateutil==1.5',
        'nydus',
    ],
    dependency_links=[
        'https://github.com/numan/nydus/tarball/7geese-nydus#egg=nydus',
    ],
    tests_require=[
        'nose',
        'mock'
    ],
    classifiers=[
        "Intended Audience :: Developers",
        'Intended Audience :: System Administrators',
        "Programming Language :: Python",
        "Topic :: Software Development",
        "Topic :: Utilities",
    ],
)
