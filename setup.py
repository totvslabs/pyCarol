from distutils.core import setup

packages = ['pycarol', 'pycarol.auth', 'pycarol.app',
            'pycarol.nlp', 'pycarol.pipeline', 'pycarol.utils']


setup(
    name='pycarol',
    version='0.1',
    packages=packages,
    maintainer='TOTVS Labs',
)
