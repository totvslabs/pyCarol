from distutils.core import setup
from setuptools import setup, find_packages

import codecs
import os
import re
import sys

here = os.path.abspath(os.path.dirname(__file__))

install_requires = [
    'pykube',
    'click==7.0',
    'dask[complete]',
    'toolz',
    'fastparquet',
    'flask==1.0.2',
    'google-auth-httplib2',
    'google-auth',
    'google-cloud-core',
    'google-cloud-storage',
    'joblib==0.11',
    'luigi',
    'numpy==1.16.3',
    'pandas==0.23.4',
    'pyarrow>=0.15.1',
    'redis==2.10.6',
    'requests',
    'tqdm',
    'urllib3',
    'deprecated',
    'pytest',
    'bumpversion',
    'python-dotenv',
    'papermill',
    'gcsfs',
    'dash',
    'dash-cytoscape',
    'colorcet',
]

def read(*parts):
    # intentionally *not* adding an encoding option to open, See:
    #   https://github.com/pypa/virtualenv/issues/201#issuecomment-3145690
    with codecs.open(os.path.join(here, *parts), 'r') as fp:
        return fp.read()

readme_note = """\
.. note::
   For the latest source, discussion, etc, please visit the
   `GitHub repository <https://github.com/totvslabs/pyCarol>`_\n\n
"""

with open('README.rst') as fobj:
    long_description = readme_note + fobj.read()

def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(
        r"^__version__ = ['\"]([^'\"]*)['\"]",
        version_file,
        re.M,
    )
    if version_match:
        return version_match.group(1)

    raise RuntimeError("Unable to find version string.")

setup(
    name='pycarol',
    packages=find_packages(exclude=['docs', 'doc']),
    version=find_version("pycarol", "__init__.py"),
    license='TOTVS',
    description='Carol Python API and Tools',
    long_description=long_description,
    author='TotvsLabs',
    maintainer='TOTVS Labs',
    author_email='ops@totvslabs.com',
    url='https://github.com/totvslabs/pyCarol',
    keywords=['Totvs', 'Carol.ai', 'AI'],
    install_requires=install_requires,
    classifiers=[
        # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
        'Development Status :: 5 - Production/Stable',
        # Define that your audience are developers
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
        "Operating System :: OS Independent",
    ],
)
