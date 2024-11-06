project(test_sql_new)

set(${PROJECT_NAME}_SOURCES
    test_new_parser_primitive.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    otterbrix::sql_new
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
