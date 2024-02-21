project(test_dataframe)

set(${PROJECT_NAME}_SOURCES
        bitmask/bitmask_tests.cpp
        column/test_column.cpp
        tools.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::dataframe
        CONAN_PKG::catch2
        CONAN_PKG::boost
        ${CMAKE_THREAD_LIBS_INIT}
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
