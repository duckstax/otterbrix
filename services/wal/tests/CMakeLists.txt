project(test_wal)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES
        test_base_wal.cpp
        test_wal.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::ql
        otterbrix::log
        otterbrix::session
        otterbrix::wal
        otterbrix::test_generaty

        CONAN_PKG::catch2
        CONAN_PKG::crc32c
        CONAN_PKG::actor-zeta
        CONAN_PKG::boost
)

target_include_directories(${PROJECT_NAME} PUBLIC
    ..
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
