from skbuild import setup

setup(
    name="otterbrix",
    version="1.0a9",
    description="""Otterbrix: computation framework for Semi-structured data processing """,
    author=" ",
    license=" ",
    packages=['otterbrix'],
    # package_dir={'': 'integration/python'},
    # package_data={"": []},
    # cmake_install_dir='integration/python',
    python_requires='>=3.6',
    # cmake_source_dir=".",
    include_package_data=True,
    extras_require={"test": ["pytest"]},
        cmake_args=[
        "-DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake",  # Pass toolchain file here
        "-DCMAKE_BUILD_TYPE=Release",
    ],
)
