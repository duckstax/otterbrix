project(test_core)

add_definitions(-DDEV_MODE)

set(${PROJECT_NAME}_SOURCES
        test_buffer.cpp
        test_scalar.cpp
        test_uvector.cpp
        )

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        CONAN_PKG::catch2
        CONAN_PKG::boost
        ottergon::assert
        rocketjoe::log
        ${CMAKE_THREAD_LIBS_INIT}
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
