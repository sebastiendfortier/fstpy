# -*- coding: utf-8 -*-
import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()


v_file = open("fstpy/VERSION")
__version__ = v_file.readline().strip()
v_file.close()

setuptools.setup(
    name="fstpy", # Replace with your own username
    version=__version__,
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
        'pandas>=1.2.4','numpy>=1.19.5','xarray>=0.19.0','dask>=2021.8.0','fstd2nc-deps >= 0.20200304.0'
    ],
    packages=setuptools.find_packages(exclude='test'),
    include_package_data=True,
    python_requires='>=3.6',
    package_data = {
    'fstpy': ['csv/*','fstpy/VERSION'],
  },
)
