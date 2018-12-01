cmake_minimum_required(VERSION 2.8.12)

set(project RocketJoe)

if (CMAKE_VERSION VERSION_LESS 3.0)
   PROJECT(${project} CXX)
else()

    cmake_policy(SET CMP0048 NEW)

    set (CMAKE_PROJECT_VERSION_MAJOR 0)
    set (CMAKE_PROJECT_VERSION_MINOR 0)
    set (CMAKE_PROJECT_VERSION_PATCH 0)
    set (CMAKE_PROJECT_VERSION "${CMAKE_PROJECT_VERSION_MAJOR}.${CMAKE_PROJECT_VERSION_MINOR}.${CMAKE_PROJECT_VERSION_PATCH}")
    PROJECT(${project} VERSION "${CMAKE_PROJECT_VERSION}" LANGUAGES CXX)
endif()

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED YES)
set(CMAKE_CXX_EXTENSIONS OFF)

#set(CMAKE_VERBOSE_MAKEFILE ON)

message (STATUS "CMAKE_BUILD_TYPE = ${CMAKE_BUILD_TYPE}")


if (ASAN)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
    message (STATUS "Building with AddressSanitizer")
endif ()

if (TSAN)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread")
    message (STATUS "Building with ThreadSanitizer")
endif ()

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DBOOST_ASIO_HAS_MOVE -DCXXOPTS_NO_RTTI" CACHE STRING "compile flags" FORCE)#-DBOOST_ASIO_NO_EXTENSIONS")

set(BUILD_LUA OFF CACHE BOOL "" FORCE)
set(ENABLE_PYTHON3 ON CACHE BOOL "" FORCE)

IF(NOT APPLE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  -pthread ")
ENDIF()

set (CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/CMakeModules")

add_compile_options(
        -v
        -Wall
        -Wextra
        -fno-rtti
)

#############################################################################################
#LUA

set(LUA_PC_CFG "lua;lua5.3;lua5.2;lua5.1" CACHE STRING "pkg-config Lua configuration file (or files, separated by ;)")

find_package(PkgConfig)
if(PKG_CONFIG_EXECUTABLE)
    pkg_search_module(LUA REQUIRED ${LUA_PC_CFG})
else()
    if(NOT LUA_INCLUDE_DIR)
        message(SEND_ERROR "LUA_INCLUDE_DIR is required if no PkgConfig is available!")
    endif()
    if(NOT LUA_LIBRARIES)
        message(SEND_ERROR "LUA_LIBRARIES is required if no PkgConfig is available!")
    endif()
endif()

find_package(Lua 5.3)

if(${LUA_INCLUDE_DIR})
    if(NOT EXISTS ${LUA_INCLUDE_DIR}/lua.h)
        message(SEND_ERROR "Can't find lua.h in ${LUA_INCLUDE_DIR}")
    endif()
endif()

include_directories(${LUA_INCLUDE_DIRS})

include_directories(SYSTEM thirdparty/sol2)

#LUA
#############################################################################################

#############################################################################################
#YAML
set(yaml_path thirdparty/yaml-cpp)
find_package(${YAML_INCLUDE_DIR} REQUIRED PATHS ${yaml_path} )
include_directories(SYSTEM thirdparty/yaml-cpp/include)
#YAML
#############################################################################################

#############################################################################################
#cxxopts
include_directories(SYSTEM thirdparty/cxxopts/include)
#cxxopts
#############################################################################################

#############################################################################################
#ACTOR-ZETA

option(STANDALONE-ACTOR-ZETA "" ON)

if(STANDALONE-ACTOR-ZETA)
    set(actor-zeta-dir ${PROJECT_SOURCE_DIR}/thirdparty/actor-zeta)
    include_directories( SYSTEM
            ${actor-zeta-dir}/core
            ${actor-zeta-dir}/io
    )
endif()

#ACTOR-ZETA
#############################################################################################

#############################################################################################
#nlohmann-json
include_directories(SYSTEM thirdparty/json/include/)
#nlohmann-json
#############################################################################################

#############################################################################################
#boost
find_package (Boost COMPONENTS REQUIRED
        system
        locale
        regex
        timer
        filesystem
        serialization

)

include_directories (${Boost_INCLUDE_DIRS})
#boost
#############################################################################################

#############################################################################################
#ccahe
find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
    message(STATUS "Enabling ccache")
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif(CCACHE_FOUND)
#ccahe
#############################################################################################

add_subdirectory(thirdparty)
add_subdirectory(api)
add_subdirectory(data_provider)
add_subdirectory(services)
add_subdirectory(apps)
add_subdirectory(tests)


set (CPACK_GENERATOR "DEB")
set (CPACK_SET_DESTDIR true)
set (CPACK_PROJECT_CONFIG_FILE "${CMAKE_MODULE_PATH}/CPackOptions.cmake" )
set (CPACK_DEB_COMPONENT_INSTALL ON)
set (CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)
set (CPACK_DEBIAN_PACKAGE_ARCHITECTURE "amd64")
set (CPACK_DEBIAN_PACKAGE_DESCRIPTION "")
set (CPACK_DEBIAN_PACKAGE_MAINTAINER "")

include (CPack)

