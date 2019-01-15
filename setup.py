from distutils.core import setup

packages = ['pycarol', 'pycarol.auth', 'pycarol.app',
            'pycarol.nlp', 'pycarol.pipeline', 'pycarol.utils',
            'pycarol.luigi_extension']

with open('requirements.txt', 'r') as req_file:
    install_requires = req_file.read()

setup(
    name='pycarol',
    version='2.7',
    description='Carol Python API',
    packages=packages,
    maintainer='TOTVS Labs',
    install_requires=install_requires.splitlines()
)
