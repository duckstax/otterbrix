project(wasm)

list(APPEND ${PROJECT_NAME}_HEADERS
        wasm.hpp
)

list(APPEND ${PROJECT_NAME}_SOURCES
        wasm.cpp
)

add_library(ottergon_${PROJECT_NAME}
            ${${PROJECT_NAME}_HEADERS}
            ${${PROJECT_NAME}_SOURCES}
)

add_library(ottergon::${PROJECT_NAME} ALIAS ottergon_${PROJECT_NAME})

set_property(TARGET ottergon_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        ottergon_${PROJECT_NAME} PRIVATE
        ottergon::log
        ottergon::wasm_runner
        CONAN_PKG::spdlog
        CONAN_PKG::actor-zeta
        CONAN_PKG::openssl
)

target_include_directories(ottergon_${PROJECT_NAME} PUBLIC "")

if (DEV_MODE)
    add_subdirectory(tests)
endif()
