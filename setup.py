#!/usr/bin/env python
from setuptools import find_packages, setup

project = "microcosm-eventsource"
version = "0.9.0"

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
    keywords="microcosm",
    install_requires=[
        "microcosm>=0.17.2",
        "microcosm-flask>=0.70.0",
        "microcosm-logging>=0.13.0",
        "microcosm-postgres>=0.23.0",
        "microcosm-pubsub>=0.28.1",
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
        "mock>=2.0.0",
        "PyHamcrest>=1.9.0",
    ],
)
