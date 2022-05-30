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

execute_process(COMMAND conan install
                --install-folder "${CMAKE_CURRENT_BINARY_DIR}/log_wasm"
                --profile:build default
                --profile:host emscripten-module
                --build missing
                "${CMAKE_CURRENT_SOURCE_DIR}/log_wasm"
                RESULT_VARIABLE RETURN_CODE)

if(NOT "${RETURN_CODE}" STREQUAL "0")
    message(FATAL_ERROR "Conan install failed='${RETURN_CODE}'")
endif()

add_custom_target("${PROJECT_NAME}_log_wasm" COMMAND conan build
                  -bf "${CMAKE_CURRENT_BINARY_DIR}/log_wasm"
                  "${CMAKE_CURRENT_SOURCE_DIR}/log_wasm")

execute_process(COMMAND conan install
                --install-folder "${CMAKE_CURRENT_BINARY_DIR}/log_quickjs_wasm"
                --profile:build default
                --profile:host emscripten-module
                --build missing
                "${CMAKE_CURRENT_SOURCE_DIR}/log_quickjs_wasm"
                RESULT_VARIABLE RETURN_CODE)

if(NOT "${RETURN_CODE}" STREQUAL "0")
    message(FATAL_ERROR "Conan install failed='${RETURN_CODE}'")
endif()

add_custom_target("${PROJECT_NAME}_log_quickjs_wasm" COMMAND conan build
                  -bf "${CMAKE_CURRENT_BINARY_DIR}/log_quickjs_wasm"
                  "${CMAKE_CURRENT_SOURCE_DIR}/log_quickjs_wasm")

add_dependencies("${PROJECT_NAME}" "${PROJECT_NAME}_log_wasm" "${PROJECT_NAME}_log_quickjs_wasm")

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
