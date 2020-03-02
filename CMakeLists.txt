#############################################################################################
# configuration

cmake_minimum_required(VERSION 3.12)

set(project RocketJoe)

cmake_policy(SET CMP0048 NEW)
set(CMAKE_PROJECT_VERSION_MAJOR 0)
set(CMAKE_PROJECT_VERSION_MINOR 0)
set(CMAKE_PROJECT_VERSION_PATCH 0)
set(CMAKE_PROJECT_VERSION "${CMAKE_PROJECT_VERSION_MAJOR}.${CMAKE_PROJECT_VERSION_MINOR}.${CMAKE_PROJECT_VERSION_PATCH}")
PROJECT(${project} VERSION "${CMAKE_PROJECT_VERSION}" LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED YES)
set(CMAKE_CXX_EXTENSIONS OFF)

#set(CMAKE_VERBOSE_MAKEFILE ON)

message (STATUS "CMAKE_BUILD_TYPE = ${CMAKE_BUILD_TYPE}")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DBOOST_ASIO_HAS_MOVE -DEXCEPTIONS_DISABLE=OFF " CACHE STRING "compile flags" FORCE)#-DBOOST_ASIO_NO_EXTENSIONS")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -rdynamic")

option(DEV_MODE "developer mode" OFF)

option(CONAN_EXPERIMENTAL_INCLUDE "CONAN EXPERIMENT INCLUDE" ON)

if (DEV_MODE)
    if(NOT WIN32)
        add_compile_options(
                -Wall
                -Wextra
                -Wpedantic

                -Wcast-align
                #-Wcast-qual
                #-Wconversion
                -Wctor-dtor-privacy
                -Wenum-compare
                #-Wfloat-equal
                -Wnon-virtual-dtor
                #-Wold-style-cast
                -Woverloaded-virtual
                -Wredundant-decls
                #-Wsign-conversion
                -Wsign-promo
        )
    endif()
endif ()

# configuration
#############################################################################################
#clang tool

include(cmake/clang_tools.cmake)

#clang tool
#############################################################################################
#conan

if(NOT EXISTS "${CMAKE_BINARY_DIR}/conan.cmake")
    message(STATUS "Downloading conan.cmake from https://github.com/conan-io/cmake-conan")
    file(DOWNLOAD "https://raw.githubusercontent.com/conan-io/cmake-conan/v0.15/conan.cmake" "${CMAKE_BINARY_DIR}/conan.cmake")
endif()

include(${CMAKE_BINARY_DIR}/conan.cmake)

if (CONAN_EXPERIMENTAL_INCLUDE)
    conan_add_remote(NAME jinncrafters INDEX 2 URL https://api.bintray.com/conan/jinncrafters/conan )
    set(CONAN_SYSTEM_INCLUDES "On")
endif ()

conan_cmake_run(
        CONANFILE conanfile.txt
        BASIC_SETUP
        BUILD missing
        NO_OUTPUT_DIRS
)

# conan
#############################################################################################
# ccahe

find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
    message(STATUS "Enabling ccache")
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif(CCACHE_FOUND)

# ccahe
#############################################################################################

include_directories(core)
add_subdirectory(core)
add_subdirectory(rocketjoed)

set(CPACK_GENERATOR "DEB")
set(CPACK_SET_DESTDIR true)
set(CPACK_PROJECT_CONFIG_FILE "${CMAKE_MODULE_PATH}/CPackOptions.cmake" )
set(CPACK_DEB_COMPONENT_INSTALL ON)
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)
set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE "amd64")
set(CPACK_DEBIAN_PACKAGE_DESCRIPTION "")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "")

include (CPack)
