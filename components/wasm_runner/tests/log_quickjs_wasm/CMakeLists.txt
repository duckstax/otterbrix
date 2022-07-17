cmake_minimum_required(VERSION 3.12 FATAL_ERROR)
PROJECT(log_wasm VERSION  LANGUAGES CXX)
enable_language(CXX)

set(CMAKE_EXECUTABLE_SUFFIX ".wasm")
set(CONAN_SYSTEM_INCLUDES TRUE)

if(EXISTS "${CMAKE_CURRENT_BINARY_DIR}/conanbuildinfo_multi.cmake")
    include("${CMAKE_CURRENT_BINARY_DIR}/conanbuildinfo_multi.cmake")
elseif(EXISTS "${CMAKE_CURRENT_BINARY_DIR}/conanbuildinfo.cmake")
    include("${CMAKE_CURRENT_BINARY_DIR}/conanbuildinfo.cmake")
else()
    message(WARNING "The file conanbuildinfo.cmake doesn't exist, you have to run conan install first")
endif()

conan_basic_setup(NO_OUTPUT_DIRS)

project("${CONAN_PACKAGE_NAME}" VERSION "${CONAN_PACKAGE_VERSION}" LANGUAGES CXX)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    add_compile_options(
        -Wall
        -Wextra
        -Wpedantic
        -Wcast-align
        -Wcast-qual
        -Wconversion
        -Wctor-dtor-privacy
        -Wdate-time
        -Wdouble-promotion
        -Wfloat-equal
        -Wformat=2
        -Wnon-virtual-dtor
        -Wold-style-cast
        -Woverloaded-virtual
        -Wredundant-decls
        -Wshadow
        -Wsign-promo
        -Wzero-as-null-pointer-constant
    )
endif()

file(GLOB_RECURSE HEADERS RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" CONFIGURE_DEPENDS "*.hpp")

file(GLOB_RECURSE SOURCES RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" CONFIGURE_DEPENDS "*.cpp")

add_executable("${CONAN_PACKAGE_NAME}" "${HEADERS}" "${SOURCES}")

target_include_directories("${CONAN_PACKAGE_NAME}" PRIVATE "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>")

conan_target_link_libraries("${CONAN_PACKAGE_NAME}")
