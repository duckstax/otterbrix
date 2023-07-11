project(sql CXX)

set(SOURCE_${PROJECT_NAME}
    lexer/lexer.cpp
    lexer/token.cpp
    parser.cpp

    parser/base/parse_error.cpp
    parser/base/parser_mask.cpp
    parser/base/parser_result.cpp
    parser/base/parser_where.cpp

    parser/common/parser_invalid.cpp

    parser/database/parser_database.cpp
    parser/database/impl/parser_database_create.cpp
    parser/database/impl/parser_database_drop.cpp

    parser/insert/parser_insert.cpp
    parser/insert/impl/parser_insert_into.cpp

    parser/select/parser_select.cpp
    parser/select/impl/parser_select_base.cpp
)

add_library(rocketjoe_${PROJECT_NAME} ${SOURCE_${PROJECT_NAME}})
add_library(rocketjoe::${PROJECT_NAME} ALIAS rocketjoe_${PROJECT_NAME})

set_property(TARGET rocketjoe_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
    rocketjoe_${PROJECT_NAME} PUBLIC
    rocketjoe::document
    rocketjoe::expressions
    rocketjoe::ql
)

target_include_directories(
    rocketjoe_${PROJECT_NAME}
    PUBLIC
)

if (DEV_MODE)
    add_subdirectory(test)
endif ()
