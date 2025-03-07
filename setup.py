import codecs
from pathlib import Path
import re

from setuptools import setup, find_packages

here = Path(__file__).absolute().parent

min_requires = [
    "beautifulsoup4",
    "deprecated",
    "deprecation",
    "gcsfs>=0.3.0,<0.7",
    "google-auth",
    "google-auth-httplib2",
    "google-cloud-bigquery>=2.26.0",
    "google-cloud-bigquery-storage",
    "google-cloud-core>=1.4.1",
    "google-cloud-storage",
    "python-dotenv",
    "requests",
    "retry",
    "tqdm",
    "urllib3",
    "pandas>=0.23.4,!=1.0.4",
    "numpy>=1.16.3",
    "pip-system-certs"
]

dataframe_requires = [
    "joblib>=0.11",
    "pyarrow>=0.15.1,<1.0.0",
]
dev_requirements = [
    "black",
    "flake8",
    "mypy",
    "pydocstyle",
    "pylint",
    "pytest",
    "sphinx-rtd-theme",
    "sphinx",
    "types-requests",
]
extras_require = {
    "dataframe": min_requires + dataframe_requires,
    "pipeline": min_requires + dataframe_requires + ["luigi", "papermill"],
    "onlineapp": min_requires + ["flask>=1.0.2", "redis"],
    "dask": min_requires + ["dask[complete]"],
    "dev": min_requires + dev_requirements,
}
extras_require["complete"] = sorted({v for req in extras_require.values() for v in req})


def read(parts):
    # intentionally *not* adding an encoding option to open, See:
    #   https://github.com/pypa/virtualenv/issues/201#issuecomment-3145690
    with codecs.open(here / parts, "r") as fp:
        return fp.read()


readme_note = """\
.. note::
   For the latest source, discussion, etc, please visit the
   `GitHub repository <https://github.com/totvslabs/pyCarol>`_\n\n
"""

with open("README.rst") as fobj:
    long_description = readme_note + fobj.read()


setup(
    name="pycarol",
    setup_requires=["wheel"],
    packages=find_packages(exclude=["docs", "doc"]),
    version="{{VERSION_PLACEHOLDER}}",
    license="TOTVS",
    description="Carol Python API and Tools",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    author="TotvsLabs",
    maintainer="TOTVS Labs",
    author_email="ops@totvslabs.com",
    url="https://github.com/totvslabs/pyCarol",
    keywords=["Totvs", "Carol.ai", "AI"],
    install_requires=min_requires,
    extras_require=extras_require,
    classifiers=[
        # Chose either "3 - Alpha",
        # "4 - Beta" or "5 - Production/Stable" as the current state of your package
        "Development Status :: 5 - Production/Stable",
        # Define that your audience are developers
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Build Tools",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.6",
        "Operating System :: OS Independent",
    ],
)
