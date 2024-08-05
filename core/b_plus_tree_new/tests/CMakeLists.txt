project(test_b_plus_tree_new)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES
        test_b_plus_tree.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::log
        otterbrix::file
        otterbrix::b_plus_tree_new
)

target_include_directories(${PROJECT_NAME} PUBLIC
    ..
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
