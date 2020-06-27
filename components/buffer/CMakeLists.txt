set(CURRENT_TARGET buffer)

list(APPEND CURRENT_${TARGET_HEADERS}
        buffer.hpp
        const_buffer.hpp
        mutable_buffer.hpp
        type_traits.hpp
)

list(APPEND CURRENT_${TARGET_HEADERS}
        buffer.cpp
        const_buffer.cpp
        mutable_buffer.cpp
)


add_library(rocketjoe_${CURRENT_TARGET} STATIC
            ${CURRENT_${TARGET_HEADERS}}
            ${CURRENT_${TARGET_HEADERS}}
)


add_library(rocketjoe::${CURRENT_TARGET} ALIAS rocketjoe_${CURRENT_TARGET})

set_property(TARGET rocketjoe_${CURRENT_TARGET} PROPERTY EXPORT_NAME ${CURRENT_TARGET})

target_link_libraries(
        rocketjoe_${CURRENT_TARGET} PRIVATE
        ${CONAN_LIBS}
)

target_include_directories(
        rocketjoe_${CURRENT_TARGET}
        PUBLIC
        "../../../"
)

if(DEV_MODE)
    add_subdirectory(tests)
endif()

install(TARGETS
        rocketjoe_${CURRENT_TARGET}

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)
