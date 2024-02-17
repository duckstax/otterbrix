project(sql CXX)

set(SOURCE_${PROJECT_NAME}
    lexer/lexer.cpp
    lexer/token.cpp
    parser.cpp

    parser/base/parse_error.cpp
    parser/base/parser_groupby.cpp
    parser/base/parser_mask.cpp
    parser/base/parser_orderby.cpp
    parser/base/parser_result.cpp
    parser/base/parser_set.cpp
    parser/base/parser_where.cpp

    parser/common/parser_invalid.cpp

    parser/database/parser_database.cpp
    parser/database/impl/parser_database_create.cpp
    parser/database/impl/parser_database_drop.cpp

    parser/delete/parser_delete.cpp
    parser/delete/impl/parser_delete_base.cpp

    parser/insert/parser_insert.cpp
    parser/insert/impl/parser_insert_into.cpp

    parser/select/parser_select.cpp
    parser/select/impl/parser_select_fields.cpp
    parser/select/impl/parser_select_from.cpp
    parser/select/impl/parser_select_without_from.cpp

    parser/update/parser_update.cpp
    parser/update/impl/parser_update_base.cpp
)

add_library(otterbrix_${PROJECT_NAME} ${SOURCE_${PROJECT_NAME}})
add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
    otterbrix_${PROJECT_NAME} PUBLIC
    otterbrix::document
    otterbrix::expressions
    otterbrix::ql
)

target_include_directories(
    otterbrix_${PROJECT_NAME}
    PUBLIC
)

if (DEV_MODE)
    add_subdirectory(test)
endif ()
