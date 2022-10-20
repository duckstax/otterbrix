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
    name="python",
    version="1.0.0",
    description='A wheel for python',
    long_description=readme(),
    license='',
    python_requires=">=3.9",
    packages=["python"],
    include_package_data=True,
    package_data={
      'python': ['python*'],
    },
    cmdclass={'install': InstallPlatlib},
    distclass=BinaryDistribution
)

