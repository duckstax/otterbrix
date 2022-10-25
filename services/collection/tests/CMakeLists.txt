project(test_collection)

add_definitions(-DDEV_MODE)

set(${PROJECT_NAME}_SOURCES
        #test_collection.cpp
        #rpc_style.cpp
        operators/test_aggregate_operators.cpp
        operators/test_group_operator.cpp
        operators/test_operators.cpp
        operators/test_get_operators.cpp
        operators/test_merge_operators.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        non_thread_scheduler
        rocketjoe::log
        rocketjoe::collection
        rocketjoe::database
        rocketjoe::cursor
        rocketjoe::session
        rocketjoe::test_generaty
)

target_include_directories(${PROJECT_NAME} PUBLIC
        ..
        )

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
