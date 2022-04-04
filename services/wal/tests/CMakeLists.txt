project(test_wal)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES
        test_base_wal.cpp
        test_wal.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        ${CONAN_LIBS}
        rocketjoe::protocol
        rocketjoe::log
        rocketjoe::wal
        rocketjoe::test_generaty
)

target_include_directories(${PROJECT_NAME} PUBLIC
    ..
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
