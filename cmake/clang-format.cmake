file(GLOB_RECURSE ALL_SOURCE_FILES *.cpp *.hpp)
find_program(CLANG_FORMAT NAMES clang-format clang-format-9 clang-format-10 )

if (CLANG_FORMAT)
    add_custom_target(
            clang-format
            COMMAND ${CLANG_FORMAT}
            -style=file
            -i
            ${ALL_SOURCE_FILES}
    )
endif ()