#!/usr/bin/env python
from setuptools import find_packages, setup


project = "microcosm-eventsource"
version = "3.0.0"

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
    python_requires=">=3.11",
    keywords="microcosm",
    install_requires=[
        "microcosm>=4.0.0",
        "microcosm-flask>=6.0.0",
        "microcosm-logging>=2.0.0",
        "microcosm-postgres>=4.0.0",
        "microcosm-pubsub>=3.0.0",
        "sqlalchemy>=2.0.0",
    ],
    setup_requires=[
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
    extras_require={
        "test": [
            "aws-encryption-sdk>=2.0.0",
            "cryptography>=35",
            "coverage>=3.7.1",
            "PyHamcrest>=1.8.5",
            "pytest-cov>=3.0.0",
            "pytest>=6.2.5",
            "pytest-cov>=5.0.0",
        ],
        "lint": [
            "flake8",
            "flake8-print",
            "flake8-isort",
        ],
        "typehinting": [
            "mypy",
            "types-psycopg2",
            "types-python-dateutil",
            "types-pytz",
            "types-setuptools",
        ],
    },
)
