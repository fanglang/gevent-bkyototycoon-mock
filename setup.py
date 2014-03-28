#!/usr/bin/python
# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

setup(
    name='gevent-bkyototycoon-mock',
    version='0.0.1',
    description='kyototycoob(binary protocol) mock server based on gevent',
    author='Studio Ousia',
    author_email='admin@ousia.jp',
    url='https://github.com/studio-ousia/gevent-bkyototycoon-mock',
    packages=find_packages(),
    license=open('LICENSE').read(),
    include_package_data=True,
    install_requires=['gevent'],
    tests_requires=['nose', 'python-kyototycoon-binary'],
    test_suite='nose.collector'
)
