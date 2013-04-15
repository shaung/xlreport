#!/usr/bin/env python
# coding: utf-8

from setuptools import setup

__VERSION__ = '0.0.1.0'

params = {
    'name': 'xlreport',
    'version': __VERSION__,
    'description': 'Simple Excel Templating',
    'author': 'shaung',
    'author_email': '_@shaung.org',
    'url': 'http://github.com/shaung/xlreport/',
    'packages':[
        'xlreport',
        'xlreport.engine',
        'xlreport.excel',
    ],
    'license': 'BSD',
    'zip_safe': False,
}

setup(**params)
