project(document)

set(${PROJECT_NAME}_SOURCES
        core/array.cpp
        core/dict.cpp
        core/doc.cpp
        core/pointer.cpp
        core/shared_keys.cpp
        core/slice_core.cpp
        core/value.cpp
        msgpack/msgpack_encoder.cpp
        mutable/mutable_array.cpp
        mutable/mutable_dict.cpp
        mutable/mutable_value.cpp
        mutable/value_slot.cpp
        support/better_assert.cpp
        support/exception.cpp
        support/num_conversion.cpp
        support/ref_counted.cpp
        support/varint.cpp

        document.cpp
        document_view.cpp
        range.cpp
        structure.cpp
        wrapper_value.cpp
)


add_library(rocketjoe_${PROJECT_NAME}
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
