from distutils.core import setup
from setuptools import setup, find_packages


with open('requirements.txt', 'r') as req_file:
    install_requires = req_file.read()

# setup(
#     name='pycarol',
#     version='2.12',
#     description='Carol Python API',
#     packages=find_packages(exclude=['docs', 'doc']),
#     maintainer='TOTVS Labs',
#     install_requires=install_requires.splitlines()
# )

setup(
    name='pycarol',
    packages=find_packages(exclude=['docs', 'doc']),
    version='2.12',
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
