project(document)

set(${PROJECT_NAME}_SOURCES
        core/array.cpp
        core/dict.cpp
        core/pointer.cpp
        core/shared_keys.cpp
        core/value.cpp
        internal/heap.cpp
        internal/value_slot.cpp
        msgpack/msgpack_encoder.cpp
        support/better_assert.cpp
        support/exception.cpp
        support/num_conversion.cpp
        support/ref_counted.cpp
        support/varint.cpp

        document.cpp
        document_view.cpp
        structure.cpp
        wrapper_value.cpp
)


add_library(otterbrix_${PROJECT_NAME}
            ${${PROJECT_NAME}_SOURCES}
)


add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PUBLIC
        CONAN_PKG::msgpack-cxx
        CONAN_PKG::boost
        dl
)

target_include_directories(
        otterbrix_${PROJECT_NAME}
        PUBLIC
        core
        json
        mutable
        support
)

if (DEV_MODE)
  add_subdirectory(test)
endif()
