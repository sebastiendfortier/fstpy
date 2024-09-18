# -*- coding: utf-8 -*-
import setuptools
from pathlib import Path
import re

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()


def get_package_version():
    init_py = Path(__file__).resolve().parent / "fstpy" / "__init__.py"
    version_regex = r"__version__\s*=\s*['\"]([^'\"]*)['\"]"
    try:
        with open(init_py, "r", encoding="utf-8") as f:
            content = f.read()
            match = re.search(version_regex, content)
            if match:
                return match.group(1)
            else:
                print("Warning: __version__ not found in __init__.py")
                return "unknown"
    except Exception as e:
        print(f"Error reading version from __init__.py: {e}")
        return "unknown"

setuptools.setup(
    name="fstpy", # Replace with your own username
    version=get_package_version(),
    author="Sebastien Fortier",
    author_email="sebastien.fortier@canada.ca",
    description="High level pandas interface to fstd files",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://gitlab.science.gc.ca/CMDS/fstpy",
    project_urls={
        "Bug Tracker": "https://gitlab.science.gc.ca/CMDS/fstpy/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU License",
        "Operating System :: OS Linux",
    ],
    install_requires=[
        'pandas>=1.2.4','numpy>=1.19.5','dask>=2021.8.0','fstd2nc-deps >= 0.20200304.0', 'cmcdict >= 2027.07.22'
    ],
    packages=setuptools.find_packages(exclude='test'),
    include_package_data=True,
    python_requires='>=3.6',
    package_data = {
    'fstpy': ['csv/*'],
  },
)
