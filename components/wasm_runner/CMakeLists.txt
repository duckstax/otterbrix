project(wasm_runner)

list(APPEND ${PROJECT_NAME}_HEADERS
        wasm.hpp
)

list(APPEND ${PROJECT_NAME}_SOURCES
        wasm.cpp
)

add_library(otterbrix_${PROJECT_NAME}
            ${${PROJECT_NAME}_HEADERS}
            ${${PROJECT_NAME}_SOURCES}
)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PUBLIC
        otterbrix::log
        CONAN_PKG::spdlog
        CONAN_PKG::proxy-wasm-cpp-host
        CONAN_PKG::boost
        CONAN_PKG::fmt
        #CONAN_PKG::openssl
)

target_include_directories(otterbrix_${PROJECT_NAME} PUBLIC "")

if (DEV_MODE)
    add_subdirectory(tests)
endif()
