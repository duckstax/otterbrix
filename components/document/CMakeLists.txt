project(document)

set(${PROJECT_NAME}_SOURCES
        container/json_object.cpp
        container/json_array.cpp
        impl/tape_builder.cpp
        impl/element.cpp
        impl/tape_ref.cpp
        impl/tape_writer.cpp
        impl/document.cpp
        impl/error.cpp
        msgpack/msgpack_encoder.cpp

        document.cpp
        json_trie_node.cpp
        string_splitter.cpp
        value.cpp
)

add_library(otterbrix_${PROJECT_NAME}
        ${${PROJECT_NAME}_SOURCES}
)


add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_include_directories(otterbrix_${PROJECT_NAME} PUBLIC "${CMAKE_SOURCE_DIR}/components/document/")

target_link_libraries(
        otterbrix_${PROJECT_NAME}
        otterbrix::types
        Boost::boost
        absl::flat_hash_map
        absl::node_hash_map
        absl::int128
        msgpackc-cxx
)

if (DEV_MODE)
    add_subdirectory(test)
    add_subdirectory(benchmark)
endif()
