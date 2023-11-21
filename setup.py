from setuptools import find_packages, setup

with open('pat/requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='pat',
    packages=['pat', 'pat.Connectors'],
    version='0.1.10',
    description='PAT Python library',
    author='Almaviva Digitaltec',
    author_email='g.ventura@almaviva.it',
    install_requires=[],
    license='MIT',
)
