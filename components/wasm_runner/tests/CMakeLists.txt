project(test_wasm_runner)

list(APPEND ${PROJECT_NAME}_SOURCES
        main.cpp
        test_log.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::log
        otterbrix::wasm_runner
        CONAN_PKG::spdlog
        CONAN_PKG::catch2
        CONAN_PKG::proxy-wasm-cpp-host
        CONAN_PKG::boost
        CONAN_PKG::fmt
        CONAN_PKG::openssl
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
