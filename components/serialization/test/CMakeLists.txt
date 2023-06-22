project(test_serialization)

add_definitions(-DDEV_MODE)

set(${PROJECT_NAME}_SOURCES
        base.cpp
        json_advanced.cpp
        json_base.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        rocketjoe::serialization
        CONAN_PKG::catch2
        CONAN_PKG::boost
        json
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
