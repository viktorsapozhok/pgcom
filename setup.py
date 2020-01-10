# -*- coding: utf-8 -*-
from os import path
from setuptools import setup

version = '0.1.1'

root_dir = path.abspath(path.dirname(__file__))

try:
    with open(
            path.join(root_dir, 'README.md'),
            mode='r',
            encoding='utf-8'
    ) as f:
        long_description = f.read()
except IOError:
    long_description = ''

version_path = path.join(root_dir, 'pgcom', 'version.py')
with open(version_path, mode='w') as f:
    f.write('__version__ = "{}"\n'.format(version))

setup(
    name='pgcom',
    version=version,
    description='PostgreSQL communication manager',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Alex Piskun',
    author_email='piskun.aleksey@gmail.com',
    url='https://github.com/viktorsapozhok/db-commuter',
    packages=['pgcom'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'pandas>=0.24.0',
        'sqlalchemy>=1.3.3',
        'psycopg2-binary>=2.7.7'
    ],
    extras_require={
        'test': ['pytest', 'tox']
    },
    python_requires='>=3.6',
)
