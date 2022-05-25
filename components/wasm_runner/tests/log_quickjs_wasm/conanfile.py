from conans import ConanFile, CMake, tools


class LogQuickJSWASM(ConanFile):
    name = f'log_quickjs_wasm'
    version = '0.1.0'
    license = 'BSD-3-Clause'
    author = 'Sergey Sherkunov <me@sanerdsher.xyz>'
    topics = ('wasm')
    settings = 'os', 'compiler', 'build_type', 'arch'
    requires = ('proxy-wasm-cpp-sdk/c32d380ca6c9b1afac38a3841be99c37af2698bf@duckstax/stable',
                'quickjs/2788d71e823b522b178db3b3660ce93689534e6d@duckstax/stable')
    generators = 'cmake'

    def configure(self):
        self.settings.compiler.cppstd = 20

    def build(self):
        cmake = CMake(self)

        cmake.configure(source_folder='src')
        cmake.build()

