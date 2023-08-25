project(ottergon-cli)

find_package(absl REQUIRED)

add_executable(${PROJECT_NAME} main.cpp)

set(ottergon_LIBS
        ottergon::log
        ottergon::database
        ottergon::collection
        ottergon::cursor
        ottergon::session
        ottergon::document
        ottergon::dispatcher
        ottergon::wal
        ottergon::disk
        ottergon::locks
        ottergon::ql
        ottergon::sql
)

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        ${ottergon_LIBS}

        cpp_ottergon

        CONAN_PKG::spdlog
        CONAN_PKG::pybind11
        CONAN_PKG::msgpack-cxx
        CONAN_PKG::date
        CONAN_PKG::crc32c
        CONAN_PKG::abseil
        CONAN_PKG::rocksdb
        CONAN_PKG::actor-zeta
        CONAN_PKG::boost
        CONAN_PKG::fmt
        CONAN_PKG::libcurl
        CONAN_PKG::zlib
        CONAN_PKG::bzip2
        #CONAN_PKG::libbacktrace
        CONAN_PKG::openssl
)