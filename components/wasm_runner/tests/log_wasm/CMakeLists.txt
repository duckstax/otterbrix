project(log_wasm)

conan_cmake_install(PATH_OR_REFERENCE "${CMAKE_CURRENT_SOURCE_DIR}"
                    INSTALL_FOLDER "${CMAKE_CURRENT_BINARY_DIR}"
                    BUILD missing
                    PROFILE_BUILD default
                    PROFILE_HOST emscripten-module)

add_custom_target("${PROJECT_NAME}" COMMAND conan build -bf "${CMAKE_CURRENT_BINARY_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}")
