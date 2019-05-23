from distutils.core import setup
from setuptools import setup, find_packages

import codecs
import os
import re
import sys

here = os.path.abspath(os.path.dirname(__file__))

try:
    with open('requirements.txt', 'r') as req_file:
        requirements = req_file.read()
    install_requires = requirements.splitlines()
except FileNotFoundError as e:
    install_requires = [
        'asn1crypto==0.24.0',
        'bokeh',
        'pykube',
        'boto3==1.9.16',
        'botocore==1.12.16',
        'cachetools==2.1.0',
        'cffi==1.11.5',
        'chardet==3.0.4',
        'click==7.0',
        'cloudpickle==0.5.6',
        'cryptography-vectors==2.3.1',
        'cryptography==2.3.1',
        'cytoolz==0.9.0.1',
        'dask==0.19.3',
        'distributed==1.23.2',
        'docutils==0.14',
        'flask==1.0.2',
        'google-api-core==1.4.1',
        'google-api-python-client==1.7.4',
        'google-auth-httplib2==0.0.3',
        'google-auth==1.5.1',
        'google-cloud-core==0.28.1',
        'google-cloud-logging==1.7.0',
        'google-cloud==0.34.0',
        'googleapis-common-protos==1.5.3',
        'grpcio==1.15.0',
        'heapdict==1.0.0',
        'httplib2==0.11.3',
        'idna==2.7',
        'itsdangerous==0.24',
        'jmespath==0.9.3',
        'joblib==0.11',
        'locket==0.2.0',
        'lockfile==0.12.2',
        'luigi',
        'markupsafe==1.1.0',
        'msgpack==0.5.6',
        'numpy==1.15.2',
        'packaging==18.0',
        'pandas==0.23.4',
        'partd==0.3.8',
        'protobuf==3.6.1',
        'psutil==5.4.7',
        'pyarrow==0.11.1',
        'pyasn1-modules==0.2.2',
        'pyasn1==0.4.4',
        'pycparser==2.19',
        'pyopenssl==18.0.0',
        'pyparsing==2.2.2',
        'pyperclip==1.7.0',
        'pysocks==1.6.8',
        'python-daemon==2.1.2',
        'python-dateutil==2.7.5',
        'pytz==2018.5',
        'redis==2.10.6',
        'requests==2.20.0',
        'rsa==4.0',
        's3fs==0.1.6',
        's3transfer==0.1.13',
        'six==1.11.0',
        'sortedcontainers==2.0.5',
        'tblib==1.3.2',
        'toolz==0.9.0',
        'tornado==4.5.3',
        'tqdm==4.28.1',
        'uritemplate==3.0.0',
        'urllib3>=1.24.2',
        'websocket-client==0.53.0',
        'werkzeug==0.14.1',
        'zict==0.1.3',
        'missingno==0.4.1'
    ]

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
