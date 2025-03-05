project(test_wal)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES
        test_base_wal.cpp
        test_wal.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        non_thread_scheduler
        otterbrix::ql
        otterbrix::log
        otterbrix::session
        otterbrix::wal
        otterbrix::test_generaty

        Catch2::Catch2
        Crc32c::crc32c
        actor-zeta::actor-zeta
        Boost::boost
)

target_include_directories(${PROJECT_NAME} PUBLIC
    ..
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
