project(new_document)

set(${PROJECT_NAME}_SOURCES
        container/json_object.cpp
        container/json_array.cpp
        impl/document.cpp
        impl/error.cpp
        msgpack/msgpack_encoder.cpp
        temp_generator/generaty.cpp

        document.cpp
        json_trie_node.cpp
        string_splitter.cpp
)
add_library(
        document_library
        ${${PROJECT_NAME}_SOURCES}
)

target_include_directories(document_library PUBLIC "${CMAKE_SOURCE_DIR}/components/new_document/")

target_link_libraries(
        document_library
        CONAN_PKG::boost
        CONAN_PKG::abseil
        CONAN_PKG::msgpack-cxx
)

if (DEV_MODE)
    add_subdirectory(test)
endif()
