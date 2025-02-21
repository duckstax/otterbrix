project(test_sql_new)

add_definitions(-DDEV_MODE)

set(${PROJECT_NAME}_SOURCES
    test_new_parser_primitive.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    otterbrix::sql_new
    otterbrix::document
    otterbrix::expressions
    otterbrix::ql
    Catch2::Catch2
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
