project(test_wasm_runner)

list(APPEND ${PROJECT_NAME}_SOURCES
        main.cpp
        test_log.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        rocketjoe::log
        rocketjoe::wasm_runner
        ${CONAN_LIBS}
)

add_subdirectory(log_wasm)
add_subdirectory(log_quickjs_wasm)

add_dependencies(${PROJECT_NAME} log_wasm log_quickjs_wasm)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
