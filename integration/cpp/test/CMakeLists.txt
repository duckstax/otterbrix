project(test_ottergon)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES
        test_collection.cpp
        test_save_load.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE

        cpp_ottergon

        rocketjoe::test_generaty
        rocketjoe::log
        rocketjoe::database
        rocketjoe::collection
        rocketjoe::cursor
        rocketjoe::session
        rocketjoe::document
        rocketjoe::dispatcher
        rocketjoe::wal
        rocketjoe::disk
        rocketjoe::locks

        CONAN_PKG::spdlog
        CONAN_PKG::msgpack-cxx
        CONAN_PKG::date
        CONAN_PKG::catch2
        CONAN_PKG::crc32c
        CONAN_PKG::abseil
        CONAN_PKG::rocksdb
        CONAN_PKG::benchmark
        CONAN_PKG::actor-zeta
        CONAN_PKG::proxy-wasm-cpp-host
        CONAN_PKG::boost
        CONAN_PKG::fmt
        CONAN_PKG::libcurl
        CONAN_PKG::zlib
        CONAN_PKG::bzip2
        CONAN_PKG::libbacktrace
        CONAN_PKG::openssl

)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
