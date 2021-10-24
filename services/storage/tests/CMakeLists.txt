project(test_storage)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES
    test_query.cpp
    test_collection.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        rocketjoe::log
        rocketjoe::storage
        rocketjoe::cursor
        rocketjoe::session
        ${CONAN_LIBS}
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
