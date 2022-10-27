project(cpp_ottergon)


set(${PROJECT_NAME}_sources
      base_spaces.cpp
      wrapper_dispatcher.cpp
)

set(ROCKETJOE_LIBS
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
)


include_directories(
        ${PROJECT_SOURCE_DIR}
)

add_library(${PROJECT_NAME} ${${PROJECT_NAME}_sources})


target_link_libraries(
        ${PROJECT_NAME} PUBLIC
        ${ROCKETJOE_LIBS}
)

set_target_properties(${PROJECT_NAME} PROPERTIES OUTPUT_NAME "ottergon")


if (DEV_MODE OR ALLOW_BENCHMARK)
    set(${PROJECT_NAME}_headers
            wrapper_dispatcher.hpp
    )

    set(${PROJECT_NAME}_sources
            wrapper_dispatcher.cpp
    )

    add_library(rocketjoe_${PROJECT_NAME}
            ${${PROJECT_NAME}_headers}
            ${${PROJECT_NAME}_sources}
    )

    add_library(rocketjoe::${PROJECT_NAME} ALIAS rocketjoe_${PROJECT_NAME})

    set_property(TARGET rocketjoe_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

    target_link_libraries(
            rocketjoe_${PROJECT_NAME} PUBLIC
          ${ROCKETJOE_LIBS}
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
endif()

if (DEV_MODE)
    add_subdirectory(test)
endif()

if (ALLOW_BENCHMARK)
    add_subdirectory(benchmark)
endif()