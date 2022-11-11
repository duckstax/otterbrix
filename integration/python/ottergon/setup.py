from skbuild import setup


setup(
    name="ottergon",
    version="0.4.0",
    description=" ",
    author=" ",
    license=" ",
    packages=['ottergon'],
    # package_dir={'': 'integration/python'},
    # package_data={"": ["libiwasm.so"]},
    # cmake_install_dir='integration/python',
    python_requires='>=3.6',
    # cmake_source_dir=".",
    include_package_data=True,
    extras_require={"test": ["pytest"]}
)
