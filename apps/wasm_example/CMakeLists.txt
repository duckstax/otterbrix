project(wasm_example)

list(APPEND ${PROJECT_NAME}_HEADERS
        services_manager.hpp
)

list(APPEND ${PROJECT_NAME}_SOURCES
        services_manager.cpp
        main.cpp
)

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES} ${${PROJECT_NAME}_HEADERS})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        ${CONAN_LIBS}
        rocketjoe::wasm
)
