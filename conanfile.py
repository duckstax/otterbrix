from conan import tools, ConanFile
from conan.tools.cmake import CMake, CMakeToolchain, cmake_layout
import os

class MyProjectConan(ConanFile):
    name = "my_project"
    version = "1.0"
    settings = "os", "compiler", "build_type", "arch"

    def configure(self):
        self.requires("boost/1.86.0@", override=True)
        self.requires("fmt/10.2.1@")
        self.requires("spdlog/1.12.0@")
        self.requires("pybind11/2.10.0@")
        self.requires("msgpack-cxx/4.1.1@")
        self.requires("catch2/2.13.7@")
        self.requires("crc32c/1.1.2@")
        self.requires("abseil/20211102.0@")
        self.requires("benchmark/1.6.1@")
        self.requires("zlib/1.2.12@")
        self.requires("bzip2/1.0.8@")
        self.requires("magic_enum/0.8.1@")
        self.requires("actor-zeta/1.0.0a12@")

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
        #"OpenSSL/*:shared": True,
    }

    def config_options(self):
        if self.settings.get_safe("compiler.cppstd") is None:
            self.settings.cppstd = 17

    #def validate(self):
    #    if not tools.check_min_cppstd(self, 17):
    #        raise tools.ConanInvalidConfiguration("")

    def layout(self):
        cmake_layout(self)

    def imports(self):
        self.copy("*.so*", dst="build_tools", src="lib")

    def generate(self):
        tc = CMakeToolchain(self)
        tc.variables["CMAKE_CXX_STANDARD"] = "17"
        tc.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
