from setuptools import setup, find_packages

with open('requirements.txt') as reqs:
    install_requires = [line for line in reqs]

CONFIG = {
    'description':'Client for ESGF Ingestion REST API',
    'version':'0.1',
    'name':'esgf-ingestion-client',
    'package_dir': {'':'lib'},
    'packages': ['esgf'],
    'install_requires': install_requires,
}

setup(**CONFIG)
