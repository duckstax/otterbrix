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

add_library(otterbrix_${PROJECT_NAME}
        ${${PROJECT_NAME}_SOURCES}
)


add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_include_directories(otterbrix_${PROJECT_NAME} PUBLIC "${CMAKE_SOURCE_DIR}/components/new_document/")

target_link_libraries(
        otterbrix_${PROJECT_NAME}
        otterbrix::types
        CONAN_PKG::boost
        CONAN_PKG::abseil
        CONAN_PKG::msgpack-cxx
)

if (DEV_MODE)
    add_subdirectory(test)
endif()
