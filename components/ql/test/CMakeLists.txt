project(test_ql)

add_definitions(-DDEV_MODE)

set(${PROJECT_NAME}_SOURCES
        base.cpp
        )

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        rocketjoe::document
        rocketjoe::ql
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
