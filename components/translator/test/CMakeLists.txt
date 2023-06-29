project(test_translator)

set(${PROJECT_NAME}_SOURCES
        test_logical_plan.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        rocketjoe::expressions
        rocketjoe::ql
        rocketjoe::logical_plan
        rocketjoe::test_generaty
        rocketjoe::translator
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
