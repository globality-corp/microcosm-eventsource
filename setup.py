#!/usr/bin/env python
from setuptools import find_packages, setup

project = "microcosm-eventsource"
version = "1.12.0"

setup(
    name=project,
    version=version,
    description="Event-sourced state machines using microcosm",
    author="Globality Engineering",
    author_email="engineering@globality.com",
    url="https://github.com/globality-corp/microcosm-eventsource",
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    include_package_data=True,
    zip_safe=False,
    python_requires=">=3.6",
    keywords="microcosm",
    install_requires=[
        "microcosm>=2.6.0",
        "microcosm-flask>=1.20.0",
        "microcosm-logging>=1.5.0",
        "microcosm-postgres>=1.14.0",
        "microcosm-pubsub>=1.20.0",
        "urllib3>=1.23,!=1.24",
    ],
    setup_requires=[
        "nose>=1.3.6",
    ],
    dependency_links=[
    ],
    entry_points={
        "microcosm.factories": [
        ],
    },
    tests_require=[
        "coverage>=3.7.1",
        "PyHamcrest>=1.9.0",
    ],
)
