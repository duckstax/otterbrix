import os
import pathlib

from setuptools import setup
from setuptools.dist import Distribution
from setuptools.command.install import install

def readme():
    with open('README.md') as f:
        return f.read()

class InstallPlatlib(install):
    def finalize_options(self):
        install.finalize_options(self)
        if self.distribution.has_ext_modules():
            self.install_lib = self.install_platlib
            
class BinaryDistribution(Distribution):
    def has_ext_modules(foo):
        return True

from setuptools import setup

setup(
    name="duck_charmer",
    version="1.0.0",
    description='A wheel for duck_charmer',
    long_description=readme(),
    license='',
    python_requires=">=3.9",
    packages=["duck_charmer"],
    include_package_data=True,
    package_data={
      'duck_charmer': ['duck_charmer*'],
    },
    cmdclass={'install': InstallPlatlib},
    distclass=BinaryDistribution
)

