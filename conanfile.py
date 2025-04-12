from conan import tools, ConanFile
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.build import check_min_cppstd
from conan.errors import ConanInvalidConfiguration
class OtterbrixConan(ConanFile):
    name = "otterbrix"
    version = "1.0"
    settings = "os", "compiler", "build_type", "arch"

    def configure(self):
        self.requires("boost/1.87.0", override=True)
        self.requires("fmt/11.1.3@")
        self.requires("spdlog/1.15.1@")
        self.requires("pybind11/2.10.0@")
        self.requires("msgpack-cxx/4.1.1@")
        self.requires("catch2/2.13.7@")
        self.requires("abseil/20230802.1@")
        self.requires("benchmark/1.6.1@")
        self.requires("zlib/1.2.12@")
        self.requires("bzip2/1.0.8@")
        self.requires("magic_enum/0.8.1@")
        self.requires("actor-zeta/1.0.0a12@")

    # options = {
    #     "actor-zeta/*:cxx_standard": [17],
    #     "actor-zeta/*:fPIC": [True, False],
    #     "actor-zeta/*:exceptions_disable": [True, False],
    #     "actor-zeta/*:rtti_disable": [True, False],
    #     # "OpenSSL/*:shared": [True, False],  # commented
    # }
    # default_options = {
    #     "actor-zeta/*:cxx_standard": 17,
    #     "actor-zeta/*:fPIC": True,
    #     "actor-zeta/*:exceptions_disable": False,
    #     "actor-zeta/*:rtti_disable": False,
    #     #"OpenSSL/*:shared": True,
    # }

    def config_options(self):
        if self.settings.get_safe("compiler.cppstd") is None:
            self.settings.cppstd = 17
        self.options["actor-zeta/*"].cxx_standard = 17
        self.options["actor-zeta/*"].fPIC = True
        self.options["actor-zeta/*"].exceptions_disable = False
        self.options["actor-zeta/*"].rtti_disable = False

    def validate(self):
        if not check_min_cppstd(self, 17):
            raise ConanInvalidConfiguration("")

    def layout(self):
        cmake_layout(self)

    def imports(self):
        self.copy("*.so*", dst="build_tools", src="lib")

    def generate(self):
        deps = CMakeDeps(self)
        deps.generate()

        tc = CMakeToolchain(self)
        tc.variables["CMAKE_CXX_STANDARD"] = "17"
        tc.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
