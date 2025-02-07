project(test_table)

set(${PROJECT_NAME}_SOURCES
        test_column.cpp
        test_table.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
    ${PROJECT_NAME} PRIVATE
    otterbrix::table
    otterbrix::file
    otterbrix::types
    otterbrix::vector
    Catch2::Catch2
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
