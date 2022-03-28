project(test_duck_charmer)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES
        spaces.cpp
        test_collection.cpp
)

set( ${PROJECT_NAME}_HEADERS
        spaces.cpp
        test_collection.cpp
        )

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES} ${${PROJECT_NAME}_HEADERS})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        ${CONAN_LIBS}
        rocketjoe::log
        rocketjoe::database
        rocketjoe::collection
        rocketjoe::cursor
        rocketjoe::session
        rocketjoe::document
        rocketjoe::dispatcher
        rocketjoe::parser
        rocketjoe::wal
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
