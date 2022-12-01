project(test_logical_plan)

set(${PROJECT_NAME}_SOURCES
        test_logical_plan.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        rocketjoe::logical_plan
        rocketjoe::expressions
        rocketjoe::ql
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
