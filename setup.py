from setuptools import setup, find_packages


with open('requirements.txt', 'r') as req_file:
    install_requires = req_file.read()

setup(
    name='pycarol',
    version='2.13',
    description='Carol Python API',
    packages=find_packages(exclude=['docs', 'doc']),
    maintainer='TOTVS Labs',
    install_requires=install_requires.splitlines()
)


