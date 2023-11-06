project(example)

include_directories(
        ${PROJECT_SOURCE_DIR}
)

add_executable(${PROJECT_NAME}  main.cpp)

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        cpp_ottergon_shared
        CONAN_PKG::spdlog
        CONAN_PKG::catch2
        rocketjoe::test_generaty
)