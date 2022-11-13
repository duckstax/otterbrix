project(convert_int_to_string)

add_executable(${PROJECT_NAME} main.cpp)

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        CONAN_PKG::spdlog
        CONAN_PKG::msgpack-cxx
        CONAN_PKG::date
        CONAN_PKG::crc32c
        CONAN_PKG::abseil
        CONAN_PKG::rocksdb
        CONAN_PKG::actor-zeta
        CONAN_PKG::proxy-wasm-cpp-host
        CONAN_PKG::boost
        CONAN_PKG::fmt
        CONAN_PKG::libcurl
        CONAN_PKG::zlib
        CONAN_PKG::bzip2
        #CONAN_PKG::libbacktrace
        CONAN_PKG::openssl
)