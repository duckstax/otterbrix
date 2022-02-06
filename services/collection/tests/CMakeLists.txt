project(test_collection)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES
    test_collection.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        rocketjoe::log
        rocketjoe::collection
        rocketjoe::cursor
        rocketjoe::session
        rocketjoe::parser
        ${CONAN_LIBS}
)

target_include_directories(${PROJECT_NAME} PUBLIC
    ..
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
