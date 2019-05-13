from distutils.core import setup
from setuptools import setup, find_packages

import codecs
import os
import re
import sys

here = os.path.abspath(os.path.dirname(__file__))

with open('requirements.txt', 'r') as req_file:
    install_requires = req_file.read()

def read(*parts):
    # intentionally *not* adding an encoding option to open, See:
    #   https://github.com/pypa/virtualenv/issues/201#issuecomment-3145690
    with codecs.open(os.path.join(here, *parts), 'r') as fp:
        return fp.read()

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
    author='TotvsLabs',
    maintainer='TOTVS Labs',
    author_email='ops@totvslabs.com',
    url='https://www.carol.ai/',
    keywords=['Totvs', 'Carol.ai', 'AI'],
    install_requires=install_requires.splitlines(),
    classifiers=[
        # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
        'Development Status :: 5 - Production/Stable',
        # Define that your audience are developers
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: MIT License',   # Again, pick a license
        'Programming Language :: Python :: 3.6',
    ],
)
