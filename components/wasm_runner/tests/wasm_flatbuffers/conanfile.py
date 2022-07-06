from conans import ConanFile, CMake, tools


class LogWASM(ConanFile):
    name = f'wasm_flatbuffers'
    version = '0.1.0'
    license = 'BSD-3-Clause'
    author = 'Nikita Makeenkov <nikita.makeenkov@gmail.com>'
    topics = ('wasm')
    settings = 'os', 'compiler', 'build_type', 'arch'
    requires = ('proxy-wasm-cpp-sdk/c32d380ca6c9b1afac38a3841be99c37af2698bf@duckstax/stable',
                'quickjs/2788d71e823b522b178db3b3660ce93689534e6d@duckstax/stable')
    generators = 'cmake'

    def configure(self):
        self.settings.compiler.cppstd = 17

    def build(self):
        cmake = CMake(self)

        cmake.configure(source_folder='.')
        cmake.build()

