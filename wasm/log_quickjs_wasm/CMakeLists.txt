cmake_minimum_required(VERSION 3.12 FATAL_ERROR)
PROJECT(log_wasm  LANGUAGES CXX)
enable_language(CXX)

set(CMAKE_EXECUTABLE_SUFFIX ".wasm")
set(CONAN_SYSTEM_INCLUDES TRUE)

include("${CMAKE_BINARY_DIR}/wasm/conanbuildinfo.cmake")
conan_basic_setup(NO_OUTPUT_DIRS)

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

add_executable("${PROJECT_NAME}" main.cpp)

target_include_directories("${PROJECT_NAME}" PRIVATE "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>")

conan_target_link_libraries("${PROJECT_NAME}")
