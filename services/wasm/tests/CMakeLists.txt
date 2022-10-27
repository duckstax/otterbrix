project(test_wasm)

set(${PROJECT_NAME}_SOURCES
        main.cpp
        test_service.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        rocketjoe::log
        rocketjoe::wasm
)

file(COPY test_files/log_wasm.wasm DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/tests)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
