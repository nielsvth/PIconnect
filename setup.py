"""A setuptools based setup module.
See:
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""

# To use a consistent encoding
import os
import sys
from codecs import open
from os import path

# Always prefer setuptools over distutils
from setuptools import find_packages, setup

HERE = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(HERE, "README.rst"), encoding="utf-8") as readme_file:
    README = readme_file.read()

LONG_DESCRIPTION = (README).replace("\r\n", "\n")

REQUIREMENTS = [
    "tzlocal",
    "numpy",
    "pytz",
    "pythonnet==2.5.1",
    "pandas",
    "future",
]
#    "enum34",

setup(
    name="PIconnect",
    version="0.0.0",
    description="Python connector to OSIsoft PI SDK",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/x-rst",
    # Author details
    author="Niels Vanthillo",
    author_email="nielsvanthillo@outlook.com",
    url="https://github.com/nielsvth/PIconnect",
    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    packages=find_packages(include=["PIconnect"]),
    include_package_data=True,
    install_requires=REQUIREMENTS,
    license="MIT license",
    zip_safe=False,
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        "Development Status :: 3 - Alpha",
        # Indicate who your project is intended for
        "Intended Audience :: Developers",
        "Topic :: Database",
        # Pick your license as you wish (should match "license" above)
        "License :: OSI Approved :: MIT License",
        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        # 'Programming Language :: Python :: 3',
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    # What does your project relate to?
    keywords="OSIsoft PI ProcessInformation PIconnect",
)
