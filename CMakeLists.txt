#############################################################################################
# configuration

cmake_minimum_required(VERSION 3.12)

cmake_policy(SET CMP0048 NEW)
set(CMAKE_PROJECT_VERSION_MAJOR 1)
set(CMAKE_PROJECT_VERSION_MINOR 0)
set(CMAKE_PROJECT_VERSION_PATCH 0)
set(CMAKE_PROJECT_VERSION "${CMAKE_PROJECT_VERSION_MAJOR}.${CMAKE_PROJECT_VERSION_MINOR}.${CMAKE_PROJECT_VERSION_PATCH}")
PROJECT(RocketJoe VERSION "${CMAKE_PROJECT_VERSION}" LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED YES)
set(CMAKE_CXX_EXTENSIONS OFF)

message (STATUS "CMAKE_BUILD_TYPE = ${CMAKE_BUILD_TYPE}")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DBOOST_ASIO_HAS_MOVE -DEXCEPTIONS_DISABLE=OFF " CACHE STRING "compile flags" FORCE)

if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")

else()
    # stack track
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -rdynamic")
endif()

option(DEV_MODE "developer mode" OFF)
option(ALLOW_BENCHMARK "Allow benchmark" OFF)

option(CONAN_EXPERIMENTAL_INCLUDE "CONAN EXPERIMENT INCLUDE" ON)

OPTION(WASM_BUILD ON)
OPTION(WASM_OBJECT "Wasm OBJECT" OFF)


set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if (DEV_MODE)
    add_definitions(-DDEV_MODE)
    enable_testing()
endif()

if (ALLOW_BENCHMARK)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
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

include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
include_directories(SYSTEM ${CONAN_INCLUDE_DIRS})
conan_basic_setup(NO_OUTPUT_DIRS)

# conan
#############################################################################################
# ccahe

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
    add_definitions(-DCMAKE_TOOLCHAIN_FILE=${CMAKE_BINARY_DIR}/build_tools/Emscripten.cmake)
    add_definitions( -DEMSCRIPTEN_ROOT_PATH=${CONAN_EMSDK_ROOT} )
endif()
# wasm
#############################################################################################

include_directories(components)
add_subdirectory(core)
add_subdirectory(components)

if (WASM_BUILD)
    add_subdirectory(wasm)
else()
    include_directories(services)
    add_subdirectory(services)
    add_subdirectory(apps)
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
