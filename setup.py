#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-pocket",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Aaron Banrnes",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_pocket"],
    install_requires=[
        "singer-python",
        "requests",
    ],
    entry_points="""
    [console_scripts]
    tap-pocket=tap_pocket:main
    """,
    packages=["tap_pocket"],
    package_data = {
        "schemas": ["tap_pocket/schemas/*.json"]
    },
    include_package_data=True,
)

