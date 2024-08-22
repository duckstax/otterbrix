from skbuild import setup

setup(
    name="otterbrix",
    version="1.0a9",
    description="""
    Otterbrix is an open-source framework for developing conventional and analytical applications.
    By adding the Otterbrix module to their applications, developers unlock the ability to quickly process unstructured and loosely structured data.
    Otterbrix seamlessly integrates with column-oriented memory format and can represent both flat and hierarchical data for efficient analytical operations. """,
    author=" ",
    license=" ",
    packages=['otterbrix'],
    # package_dir={'': 'integration/python'},
    # package_data={"": []},
    # cmake_install_dir='integration/python',
    python_requires='>=3.6',
    # cmake_source_dir=".",
    include_package_data=True,
    extras_require={"test": ["pytest"]}
)
