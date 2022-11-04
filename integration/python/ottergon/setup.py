import subprocess
from skbuild import setup

import skbuild.constants

build_dir = skbuild.constants.CMAKE_BUILD_DIR()

subprocess.run([f"conan install . -s build_type=Release -if={build_dir}"], shell=True, check=True)

setup(
    name="ottergon",
    version="1.0.0",
    description=" ",
    author=" ",
    license=" ",
    packages=['ottergon'],
    # package_dir={'': 'integration/python'},
    # package_data={"": ["libiwasm.so"]},
    # cmake_install_dir='integration/python',
    python_requires='>=3.6',
    # cmake_source_dir=".",
    include_package_data=True
)
