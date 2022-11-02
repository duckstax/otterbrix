cmake_minimum_required(VERSION 3.20 FATAL_ERROR)

set(CMAKE_PROJECT_VERSION_MAJOR 1)
set(CMAKE_PROJECT_VERSION_MINOR 0)
set(CMAKE_PROJECT_VERSION_PATCH 0)
set(CMAKE_PROJECT_VERSION "${CMAKE_PROJECT_VERSION_MAJOR}.${CMAKE_PROJECT_VERSION_MINOR}.${CMAKE_PROJECT_VERSION_PATCH}")
PROJECT(ottergon VERSION "${CMAKE_PROJECT_VERSION}" LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED YES)
set(CMAKE_CXX_EXTENSIONS OFF)

message (STATUS "CMAKE_BUILD_TYPE = ${CMAKE_BUILD_TYPE}")
message(STATUS "ARCH : ${CMAKE_SYSTEM_NAME}")

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DBOOST_ASIO_HAS_MOVE -DEXCEPTIONS_DISABLE=OFF " CACHE STRING "compile flags" FORCE)

option(DEV_MODE "developer mode" OFF)
option(ALLOW_BENCHMARK "Allow benchmark" OFF)
option(CONAN_EXPERIMENTAL_INCLUDE "CONAN EXPERIMENT INCLUDE" ON)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if (DEV_MODE)
    add_definitions(-DDEV_MODE)
    enable_testing()
endif()

if (ALLOW_BENCHMARK)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
endif()

if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")

elseif(${CMAKE_SYSTEM_NAME} MATCHES "Emscripten")
    set(WASM_BUILD ON)
else()
    # stack track
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -rdynamic")
endif()

if ((CMAKE_CXX_COMPILER_ID MATCHES "GNU") OR (CMAKE_CXX_COMPILER_ID MATCHES "Clang"))
    add_compile_options(

            #-Werror

            -Wall
            -Wextra
            -Wpedantic

            -Wcast-align
            -Wcast-qual
            -Wconversion
            -Wctor-dtor-privacy
            -Wenum-compare
            -Wfloat-equal
            -Wnon-virtual-dtor
            -Wold-style-cast
            -Woverloaded-virtual
            -Wredundant-decls
            -Wsign-conversion
            -Wsign-promo
    )
elseif (CMAKE_CXX_COMPILER_ID MATCHES "MSVC")
    add_compile_options(/W4 /WX)
endif ()

# configuration
#############################################################################################
#clang tools

option(ADDRESS_SANITIZER "Enable Clang AddressSanitizer" OFF)

if(ADDRESS_SANITIZER)
    include(cmake/address_sanitizer.cmake)
endif()

include(cmake/clang-format.cmake)

#clang tools
#############################################################################################
#conan

if(WASM_BUILD)
    include(${CMAKE_BINARY_DIR}/wasm/conanbuildinfo.cmake)
else()
    include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
endif()

#include(${CMAKE_BINARY_DIR}/conan_paths.cmake)
set(CMAKE_MODULE_PATH ${CMAKE_BINARY_DIR} ${CMAKE_MODULE_PATH})

include_directories(SYSTEM ${CONAN_INCLUDE_DIRS})
conan_basic_setup(NO_OUTPUT_DIRS TARGETS KEEP_RPATHS)

# conan
#############################################################################################
# ccahe

# Since we are in Conan TARGETS mode,
# use a list of CONAN_PKG targets instead of CONAN_LIBS
#set(COMMON_COMPONENTS
#  CONAN_PKG::actor-zeta CONAN_PKG::proxy-wasm-cpp-host
#  CONAN_PKG::msgpack-cxx CONAN_PKG::date CONAN_PKG::crc32c
#  CONAN_PKG::abseil CONAN_PKG::rocksdb CONAN_PKG::benchmark
#  CONAN_PKG::boost CONAN_PKG::fmt CONAN_PKG::libcurl CONAN_PKG::zlib
#   CONAN_PKG::libbacktrace CONAN_PKG::openssl
#)


find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
    message(STATUS "Using ccache: ${CCACHE_FOUND}")
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ${CCACHE_FOUND})
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ${CCACHE_FOUND})
    set(ENV{CCACHE_COMMENTS} "1")
endif(CCACHE_FOUND)

# ccahe
#############################################################################################
# wasm
if(WASM_BUILD)
    set(WASM_OBJECT ON)
endif()
# wasm
#############################################################################################

#set(CMAKE_BUILD_WITH_INSTALL_RPATH ON)
#set( CMAKE_SKIP_RPATH ON)
set(CMAKE_SKIP_BUILD_RPATH FALSE)

# when building, don't use the install RPATH already
# (but later on when installing)
set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)

#set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")

# add the automatically determined parts of the RPATH
# which point to directories outside the build tree to the install RPATH
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

include_directories(components)
add_subdirectory(core)
add_subdirectory(components)

if (WASM_BUILD)
    add_subdirectory(wasm)
else()
    include_directories(services)
    add_subdirectory(services)
    add_subdirectory(integration)
endif ()



#set(CPACK_GENERATOR "DEB")
#set(CPACK_SET_DESTDIR true)
#set(CPACK_PROJECT_CONFIG_FILE "${CMAKE_MODULE_PATH}/CPackOptions.cmake" )
#set(CPACK_DEB_COMPONENT_INSTALL ON)
#set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)#
#set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE "amd64")
#set(CPACK_DEBIAN_PACKAGE_DESCRIPTION "")
#set(CPACK_DEBIAN_PACKAGE_MAINTAINER "")


#include (CPack)
