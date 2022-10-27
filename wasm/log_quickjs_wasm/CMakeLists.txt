PROJECT(log_quickjs_wasm)
set(CMAKE_EXECUTABLE_SUFFIX ".wasm")

add_executable("${PROJECT_NAME}" main.cpp)

target_link_options( ${PROJECT_NAME}  PRIVATE "SHELL:--no-entry -s STANDALONE_WASM -s PURE_WASI=1 -s ABORTING_MALLOC=0 -s EXPORTED_FUNCTIONS=['_malloc'] -s ERROR_ON_UNDEFINED_SYMBOLS=0" )

target_include_directories(${PROJECT_NAME} PUBLIC "")

target_link_libraries(
        ${PROJECT_NAME}  PRIVATE
        CONAN_PKG::msgpack-cxx CONAN_PKG::proxy-wasm-cpp-sdk CONAN_PKG::quickjs CONAN_PKG::boost
)

set_target_properties( ${PROJECT_NAME}
        PROPERTIES
        ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib"
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
        )


set_target_properties( ${PROJECT_NAME} PROPERTIES LINK_SEARCH_START_STATIC ON)
set_target_properties( ${PROJECT_NAME} PROPERTIES LINK_SEARCH_END_STATIC ON)

target_link_options( ${PROJECT_NAME} PRIVATE -static-libgcc -static-libstdc++ -static)