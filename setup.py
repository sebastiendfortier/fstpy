import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()


v_file = open("VERSION")
__version__ = v_file.readline()
v_file.close()

setuptools.setup(
    name="fstpy", # Replace with your own username
    version=__version__,
    author="Sebastien Fortier",
    author_email="sebastien.fortier@canada.ca",
    description="A small example package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://gitlab.science.gc.ca/cmds/fstpy",
    project_urls={
        "Bug Tracker": "https://gitlab.science.gc.ca/cmds/fstpy/issues",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU License",
        "Operating System :: OS Linux",
    ],
    install_requires=[
        numpy>=1.19.5,
        pandas>=1.0.0,
    ],
    package_dir={"": "fstpy"},
    packages=setuptools.find_packages(where="fstpy"),
    python_requires=">=3.6",
)