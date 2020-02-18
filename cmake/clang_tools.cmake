file(GLOB_RECURSE ALL_SOURCE_FILES *.cpp *.hpp)

find_program(CLANG_TIDY NAMES clang-tidy )
if (CLANG_TIDY)
    add_custom_target(
            clang-tidy
            COMMAND ${CLANG_TIDY}
            ${ALL_SOURCE_FILES}
            --
            -std=c++14
            ${INCLUDE_DIRECTORIES}
    )
endif ()

find_program(CLANG_FORMAT NAMES clang-format )
if (CLANG_FORMAT)
    add_custom_target(
            clang-format
            COMMAND ${CLANG_FORMAT}
            -style=file
            -i
            ${ALL_SOURCE_FILES}
    )
endif ()

option(ADDRESS_SANITIZER "Enable Clang AddressSanitizer" OFF)

if (ADDRESS_SANITIZER)
    message(STATUS "AddressSanitizer enabled for build")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O1 -fno-omit-frame-pointer -fsanitize=address")
endif ()

option(UNDEFINED_SANITIZER "Enable Clang UndefinedBehaviorSanitizer" OFF)

if (UNDEFINED_SANITIZER)
    message(STATUS "UndefinedBehaviorSanitizer enabled for build")
    set(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -fsanitize=undefined -fsanitize=integer")
endif ()

option(CLANG_CODE_COVERAGE "Enable code coverage metrics in Clang" OFF)

if (CLANG_CODE_COVERAGE)
    message(STATUS "Code coverage metrics enabled for build")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-instr-generate -fcoverage-mapping")
endif ()