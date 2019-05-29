"""Setup Script"""

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="crckafka",
    version="0.0.1",
    author="Sunil Samal",
    author_email="ssamal@redhat.com",
    description="CRA wrapper for kafka",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license='APACHE 2.0',
    url="https://github.com/sunilk747/crc_kafka",
    packages=setuptools.find_packages(exclude=['tests']),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)