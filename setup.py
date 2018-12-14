from setuptools import setup, find_packages

setup(
    name='pycarol',
    version='0.1',
    packages=find_packages(exclude=['docs', 'doc']),
    maintainer='TOTVS Labs'
)
