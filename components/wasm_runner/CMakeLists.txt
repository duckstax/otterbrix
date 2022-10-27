project(wasm_runner)

list(APPEND ${PROJECT_NAME}_HEADERS
        wasm.hpp
)

list(APPEND ${PROJECT_NAME}_SOURCES
        wasm.cpp
)

add_library(rocketjoe_${PROJECT_NAME}
            ${${PROJECT_NAME}_HEADERS}
            ${${PROJECT_NAME}_SOURCES}
)

add_library(rocketjoe::${PROJECT_NAME} ALIAS rocketjoe_${PROJECT_NAME})

set_property(TARGET rocketjoe_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        rocketjoe_${PROJECT_NAME} PUBLIC
        rocketjoe::log
        CONAN_PKG::spdlog
        CONAN_PKG::proxy-wasm-cpp-host
        CONAN_PKG::boost
        CONAN_PKG::fmt
        CONAN_PKG::openssl
)

target_include_directories(rocketjoe_${PROJECT_NAME} PUBLIC "")

if (DEV_MODE)
    add_subdirectory(tests)
endif()
