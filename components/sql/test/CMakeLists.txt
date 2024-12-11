project(test_sql)

set(${PROJECT_NAME}_SOURCES
    test_lexer.cpp
    test_parser.cpp
    test_parser_insert.cpp
    test_parser_select.cpp
    test_parser_delete.cpp
    test_parser_update.cpp
    test_parser_join.cpp
    test_index_parser.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    otterbrix::sql
    Catch2::Catch2
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
