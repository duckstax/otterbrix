project(document)

set( ${PROJECT_NAME}_HEADERS
        document.hpp
        document_id.hpp
        document_view.hpp
        range.hpp
        structure.hpp
        core/array.hpp
        core/base.hpp
        core/dict.hpp
        core/doc.hpp
        core/internal.hpp
        core/pointer.hpp
        core/shared_keys.hpp
        core/slice_core.hpp
        core/slice.hpp
        core/value.hpp
        json/json_coder.hpp
        mutable/mutable_array.h
        mutable/mutable_array.hpp
        mutable/mutable_dict.h
        mutable/mutable_dict.hpp
        mutable/mutable_value.hpp
        mutable/value_slot.hpp
        support/better_assert.hpp
        support/endian.hpp
        support/endianness.hpp
        support/exception.hpp
        support/function_ref.hpp
        support/num_conversion.hpp
        support/parse_date.hpp
        support/platform_compat.hpp
        support/ref_counted.hpp
        support/slice_io.hpp
        support/small_vector_base.hpp
        support/small_vector.hpp
        support/string_table.hpp
        support/temp_array.hpp
        support/varint.hpp
        support/writer.hpp
)

set(${PROJECT_NAME}_SOURCES
        document.cpp
        document_view.cpp
        range.cpp
        structure.cpp
        core/array.cpp
        core/dict.cpp
        core/doc.cpp
        core/pointer.cpp
        core/shared_keys.cpp
        core/slice_core.cpp
        core/value.cpp
        json/json_coder.cpp
        msgpack/msgpack_encoder.cpp
        mutable/mutable_array.cpp
        mutable/mutable_dict.cpp
        mutable/mutable_value.cpp
        mutable/value_slot.cpp
        support/better_assert.cpp
        support/exception.cpp
        support/num_conversion.cpp
        support/parse_date.cpp
        support/ref_counted.cpp
        support/slice_io.cpp
        support/string_table.cpp
        support/varint.cpp
        support/writer.cpp
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
        core
        json
        mutable
        support
)

if (DEV_MODE)
    add_subdirectory(test)
endif()
