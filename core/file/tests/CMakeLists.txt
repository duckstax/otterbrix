project(test_file_system)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES
        test_file_system.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        non_thread_scheduler
        otterbrix::ql
        otterbrix::log
        otterbrix::file
)

target_include_directories(${PROJECT_NAME} PUBLIC
    ..
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
