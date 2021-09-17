project(storage_base CXX)

set( ${PROJECT_NAME}_HEADERS
        document.hpp
        core/array.hpp
        core/base.hpp
        core/deep_iterator.hpp
        core/dict.hpp
        core/doc.hpp
        core/encoder.hpp
        core/internal.hpp
        core/json_converter.hpp
        core/json_delta.hpp
        core/mutable.hpp
        core/path.hpp
        core/pointer.hpp
        core/shared_keys.hpp
        core/slice_core.hpp
        core/slice.hpp
        core/storage.h
        core/storage.hpp
        core/storage_impl.hpp
        core/value.hpp
        mutable/mutable_array.h
        mutable/mutable_array.hpp
        mutable/mutable_dict.h
        mutable/mutable_dict.hpp
        mutable/mutable_value.hpp
        mutable/value_slot.hpp
        support/better_assert.hpp
        support/bitmap.hpp
        support/concurrent_arena.hpp
        support/concurrent_map.hpp
        support/delimiter.hpp
        support/diff_match_patch.hpp
        support/endian.hpp
        support/endianness.hpp
        support/exception.hpp
        support/function_ref.hpp
        support/json5.hpp
        support/json_encoder.hpp
        support/num_conversion.hpp
        support/parse_date.hpp
        support/platform_compat.hpp
        support/ref_counted.hpp
        support/slice_io.hpp
        support/slice_stream.hpp
        support/small_vector_base.hpp
        support/small_vector.hpp
        support/string_table.hpp
        support/temp_array.hpp
        support/varint.hpp
        support/writer.hpp
        tree/hash_tree.hpp
        tree/hash_tree_internal.hpp
        tree/key_tree.hpp
        tree/mutable_hash_tree.hpp
        tree/mutable_node.hpp
        tree/node_ref.hpp
        )

set(${PROJECT_NAME}_SOURCES
        document.cpp
        core/array.cpp
        core/deep_iterator.cpp
        core/dict.cpp
        core/doc.cpp
        core/encoder.cpp
        core/json_converter.cpp
        core/json_delta.cpp
        core/path.cpp
        core/pointer.cpp
        core/shared_keys.cpp
        core/slice_core.cpp
        core/storage.cpp
        core/value.cpp
        core/value_dump.cpp
        mutable/mutable_array.cpp
        mutable/mutable_dict.cpp
        mutable/mutable_value.cpp
        mutable/value_slot.cpp
        support/better_assert.cpp
        support/bitmap.cpp
        support/concurrent_arena.cpp
        support/concurrent_map.cpp
        support/exception.cpp
        support/json5.cpp
        support/json_encoder.cpp
        support/num_conversion.cpp
        support/parse_date.cpp
        support/ref_counted.cpp
        support/slice_io.cpp
        support/slice_stream.cpp
        support/string_table.cpp
        support/varint.cpp
        support/writer.cpp
        tree/hash_tree.cpp
        tree/key_tree.cpp
        tree/mutable_hash_tree.cpp
        tree/node_ref.cpp
)

add_library(rocketjoe_${PROJECT_NAME}
            ${${PROJECT_NAME}_HEADERS}
            ${${PROJECT_NAME}_SOURCES}
)


add_library(rocketjoe::${PROJECT_NAME} ALIAS rocketjoe_${PROJECT_NAME})

set_property(TARGET rocketjoe_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        rocketjoe_${PROJECT_NAME} PRIVATE
        ${CONAN_LIBS}
        boost_json
        dl
)

target_include_directories(
        rocketjoe_${PROJECT_NAME}
        PUBLIC
        "${PROJECT_SOURCE_DIR}/components/storage/"
        core
        mutable
        support
        tree
)

add_subdirectory(test)


if (DEV_MODE)
#    add_subdirectory(test)
endif()
