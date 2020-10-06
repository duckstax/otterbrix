file(GLOB_RECURSE ALL_SOURCE_FILES *.cpp *.hpp)
find_program(CLANG_TIDY NAMES clang-tidy clang-tidy-9 clang-tidy-10 )

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