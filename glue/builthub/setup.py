import setuptools
 
with open("README.md", "r") as fh:
    long_description = fh.read()
 
setuptools.setup(
    name="builthub",
    version="2.0",
    author="kevin, abel",
    author_email="kmorabui@emeal.nttdata.com, abel.munoz.alcaraz@nttdata.com",
    description="BuiltHub.py package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)