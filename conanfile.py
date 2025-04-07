from conan import ConanFile
from conan.tools.cmake import CMake, CMakeToolchain, cmake_layout
import os

class MyProjectConan(ConanFile):
    name = "my_project"
    version = "1.0"
    settings = "os", "compiler", "build_type", "arch"
    requires = [
        "boost/1.86.0",
        "fmt/10.2.1",
        "spdlog/1.12.0",
        "pybind11/2.13.6",
        "msgpack-cxx/4.1.1",
        "catch2/2.13.7",  # catch2/3.4.0 is commented
        "crc32c/1.1.2",
        "abseil/20230802.1",
        "benchmark/1.6.1",
        "zlib/1.2.12",
        "bzip2/1.0.8",
        "magic_enum/0.8.1",
        "actor-zeta/1.0.0a11@duckstax/stable"
    ]
    options = {
        "actor-zeta/*:cxx_standard": [17],
        "actor-zeta/*:fPIC": [True, False],
        "actor-zeta/*:exceptions_disable": [True, False],
        "actor-zeta/*:rtti_disable": [True, False],
        # "OpenSSL/*:shared": [True, False],  # commented
    }
    default_options = {
        "actor-zeta/*:cxx_standard": 17,
        "actor-zeta/*:fPIC": True,
        "actor-zeta/*:exceptions_disable": False,
        "actor-zeta/*:rtti_disable": False,
        # "OpenSSL/*:shared": True,
    }
    generators = "CMakeDeps", "CMakeToolchain"

    def layout(self):
        cmake_layout(self)

    def imports(self):
        self.copy("*.so*", dst="build_tools", src="lib")

    def generate(self):
        tc = CMakeToolchain(self)
        tc.generate()
        deps = CMakeDeps(self)
        deps.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
