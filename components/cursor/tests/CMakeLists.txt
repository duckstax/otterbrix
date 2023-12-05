project(test_cursor)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES
        test_cursor.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::log
        otterbrix::document
        otterbrix::test_generaty
        otterbrix::cursor
        CONAN_PKG::boost
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
