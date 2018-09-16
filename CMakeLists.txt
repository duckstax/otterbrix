cmake_minimum_required(VERSION 2.8)

set(project RocketJoe)

#set (CMAKE_PROJECT_VERSION_MAJOR 1)
#set (CMAKE_PROJECT_VERSION_MINOR 0)
#set (CMAKE_PROJECT_VERSION_PATCH 0)
#set (CMAKE_PROJECT_VERSION "${CMAKE_PROJECT_VERSION_MAJOR}.${CMAKE_PROJECT_VERSION_MINOR}.${CMAKE_PROJECT_VERSION_PATCH}")

#if (CMAKE_VERSION VERSION_LESS 3.0)
#   PROJECT(${project} CXX)
#else()
#    cmake_policy(SET CMP0048 NEW)
#    PROJECT(${project} VERSION "${CMAKE_PROJECT_VERSION}" LANGUAGES CXX)
#endif()

project(${project} CXX)

set(CMAKE_CXX_STANDARD 14)


message (STATUS "CMAKE_BUILD_TYPE = ${CMAKE_BUILD_TYPE}")


if (ASAN)
    SET (CMAKE_CXX_COMPILER "/usr/bin/clang++")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")

    message (STATUS "Building with AddressSanitizer")
endif ()

if (TSAN)
    SET (CMAKE_CXX_COMPILER "/usr/bin/clang++")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread")

    message (STATUS "Building with ThreadSanitizer")
endif ()

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -std=c++14 -DBOOST_ASIO_HAS_MOVE ")#-DBOOST_ASIO_NO_EXTENSIONS")

IF(NOT APPLE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  -pthread ")
ENDIF()

set (CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/CMakeModules")

#############################################################################################
#YAML
set(yaml_path thirdparty/yaml-cpp)
find_package(${YAML_INCLUDE_DIR} REQUIRED PATHS ${yaml_path} )
include_directories(SYSTEM thirdparty/yaml-cpp/include)
#YAML
#############################################################################################

#############################################################################################
#nlohmann-json
include_directories(SYSTEM thirdparty/json/include/)
#nlohmann-json
#############################################################################################

#############################################################################################
#BOOST
find_package (Boost COMPONENTS REQUIRED
        system
        locale
        regex
        timer
        program_options
        filesystem
        serialization

)

include_directories (${Boost_INCLUDE_DIRS})
#BOOST
#############################################################################################

#############################################################################################
#CCACHE
find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif(CCACHE_FOUND)
#CCACHE
#############################################################################################



add_subdirectory(thirdparty)
add_subdirectory(api)
add_subdirectory(services)
add_subdirectory(apps)

include_directories(SYSTEM thirdparty/json/include)


set (CPACK_GENERATOR "DEB")
set (CPACK_SET_DESTDIR true)
set (CPACK_PROJECT_CONFIG_FILE "${CMAKE_MODULE_PATH}/CPackOptions.cmake" )
set (CPACK_DEB_COMPONENT_INSTALL ON)
set (CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)
set (CPACK_DEBIAN_PACKAGE_ARCHITECTURE "amd64")
set (CPACK_DEBIAN_PACKAGE_DESCRIPTION "")
set (CPACK_DEBIAN_PACKAGE_MAINTAINER "")

include (CPack)

