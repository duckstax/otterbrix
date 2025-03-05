project(test_collection)

add_definitions(-DDEV_MODE)

set(${PROJECT_NAME}_SOURCES
        #test_collection.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        non_thread_scheduler
        otterbrix::log
        otterbrix::collection
        otterbrix::memory_storage
        otterbrix::cursor
        otterbrix::session
        otterbrix::test_generaty
        spdlog::spdlog
        absl::int128
        Boost::boost
        Catch2::Catch2
        actor-zeta::actor-zeta
)

target_include_directories(${PROJECT_NAME} PUBLIC
        ..
        )

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
