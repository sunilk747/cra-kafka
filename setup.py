"""Setup Script"""

import setuptools

def _get_requirements():
    with open('requirements.txt') as _file:
        return _file.readlines()


with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="crakafka",
    version="0.0.1",
    author="Sunil Samal",
    author_email="ssamal@redhat.com",
    description="CRA wrapper for kafka",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license='APACHE 2.0',
    url="https://github.com/sunilk747/cra-kafka",
    packages=setuptools.find_packages(exclude=['tests']),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    install_requires=_get_requirements(),
)