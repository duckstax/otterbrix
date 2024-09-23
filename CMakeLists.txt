cmake_minimum_required(VERSION 3.16.3 FATAL_ERROR)

set(CMAKE_PROJECT_VERSION_MAJOR 1)
set(CMAKE_PROJECT_VERSION_MINOR 0)
set(CMAKE_PROJECT_VERSION_PATCH 0)
set(CMAKE_PROJECT_VERSION "${CMAKE_PROJECT_VERSION_MAJOR}.${CMAKE_PROJECT_VERSION_MINOR}.${CMAKE_PROJECT_VERSION_PATCH}")
PROJECT(otterbrix VERSION "${CMAKE_PROJECT_VERSION}" LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED YES)
set(CMAKE_CXX_EXTENSIONS OFF)

message(STATUS "ARCH : ${CMAKE_SYSTEM_NAME}")
message(STATUS "CMAKE_CXX_COMPILER_ID = ${CMAKE_CXX_COMPILER_ID}")
message(STATUS "CMAKE_C_COMPILER_ID = ${CMAKE_C_COMPILER_ID}")
message(STATUS "STANDARD CPP = ${CMAKE_CXX_STANDARD}")
message(STATUS "CMAKE_BUILD_TYPE = ${CMAKE_BUILD_TYPE}")

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DBOOST_ASIO_HAS_MOVE -DEXCEPTIONS_DISABLE=OFF " CACHE STRING "compile flags" FORCE)

option(DEV_MODE "developer mode" OFF)
option(EXAMPLE "Example" OFF)
option(ALLOW_BENCHMARK "Allow benchmark" OFF)
option(CONAN_EXPERIMENTAL_INCLUDE "CONAN EXPERIMENT INCLUDE" ON)
option(APPLICATION_SERVER "application server" OFF)

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

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")
find_package(Sanitizers REQUIRED)

# TODO remove
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g -fno-omit-frame-pointer -fsanitize=thread")
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g -fno-omit-frame-pointer -fsanitize=address")
# set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")


#clang tools
#############################################################################################
if ((CMAKE_CXX_COMPILER_ID MATCHES "Clang") AND NOT APPLE) #------------------------------------- Clang
    ############## LLVM CONFIGURATION #################
    # https://github.com/nsumner/llvm-demo Example LLVM Build

    # LLVM_DIR must be set to the prefix of /share/llvm/cmake via commandline
    find_package(LLVM REQUIRED CONFIG)
    message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION} at ${LLVM_LIBRARY_DIRS}")

    find_library(CLANG_LIBCPP
            NAMES libc++ c++
            HINTS ${LLVM_LIBRARY_DIRS})

    find_library(CLANG_LIBCPP_ABI
            NAMES libc++abi c++abi
            HINTS ${LLVM_LIBRARY_DIRS})

    message(STATUS "Found CLANG_LIBCPP=${CLANG_LIBCPP}")
    message(STATUS "Found CLANG_LIBCPP_ABI=${CLANG_LIBCPP_ABI}")

    # Linker flags for targets and Conan
    set(LINKFLAGS "")
    ### https://stackoverflow.com/questions/43662736/cmake-switch-between-clang-g-and-libc-libstdc

    if (${CLANG_LIBCPP} MATCHES "CLANG_LIBCPP-NOTFOUND")
        set(LIBCXX "libstdc++11")
    else()
        # Change compiler library and libunwind from libgcc GCC implementation to LLVM's compiler-rt
        # See https://clang.llvm.org/docs/Toolchain.html for details
        # LINKFLAGS variable contains required link flags
        set(LINKFLAGS "-stdlib=libc++ -rtlib=compiler-rt")

        if (NOT APPLE)
            find_library(
                    LIBUNWIND_LIBRARY
                    NAMES libunwind.a
                    PATH_SUFFIXES lib lib64)

            if (LIBUNWIND_LIBRARY)
                message(STATUS "libunwind found: ${LIBUNWIND_LIBRARY}, using libunwind")
                set(LINKFLAGS "${LINKFLAGS} -unwindlib=libunwind")
            else()
                message(STATUS "libunwind not found, using libgcc")
                set(LINKFLAGS "${LINKFLAGS} -unwindlib=libgcc -lgcc_s")
            endif()
        endif()
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${LINKFLAGS}")
        set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${LINKFLAGS}")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
        set(LIBCXX "libc++")
    endif()

    if (CLANG_LIBCPP_ABI MATCHES "CLANG_LIBCPP_ABI-NOTFOUND")
    else()
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lc++abi")
    endif()

else() #---------------------------------------------------------------------------- GNU
    set(LIBCXX "libstdc++11")
endif() #--------------------------------------------------------------------------- Clang
#############################################################################################
#conan

include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)

#include(${CMAKE_BINARY_DIR}/conan_paths.cmake)
set(CMAKE_MODULE_PATH ${CMAKE_BINARY_DIR} ${CMAKE_MODULE_PATH})

include_directories(SYSTEM ${CONAN_INCLUDE_DIRS})
conan_basic_setup(NO_OUTPUT_DIRS TARGETS KEEP_RPATHS)

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang"  AND UNIX AND NOT APPLE )
    # https://docs.conan.io/1/reference/config_files/settings.yml.html#c-standard-libraries-aka-compiler-libcxx
    #conan_cmake_autodetect(CONAN_SETTINGS)
    set(CONAN_SETTINGS "build_type=${CMAKE_BUILD_TYPE};compiler.libcxx=${LIBCXX};compiler.cppstd=${CMAKE_CXX_STANDARD}")
endif ()

message(STATUS "CONAN_SETTINGS: ${CONAN_SETTINGS}")

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

if (APPLICATION_SERVER)
    set(WASM_OBJECT OFF)
    set(WASM_BUILD OFF)
endif ()

include_directories(services)
add_subdirectory(services)
add_subdirectory(integration)
if(EXAMPLE)
    # add_subdirectory(example)
endif()



#set(CPACK_GENERATOR "DEB")
#set(CPACK_SET_DESTDIR true)
#set(CPACK_PROJECT_CONFIG_FILE "${CMAKE_MODULE_PATH}/CPackOptions.cmake" )
#set(CPACK_DEB_COMPONENT_INSTALL ON)
#set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)#
#set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE "amd64")
#set(CPACK_DEBIAN_PACKAGE_DESCRIPTION "")
#set(CPACK_DEBIAN_PACKAGE_MAINTAINER "")


#include (CPack)
