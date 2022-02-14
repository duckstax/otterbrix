set(CURRENT_TARGET btree_storage)

list(APPEND CURRENT_TARGET_HEADERS
        btree_storage.hpp
        range.hpp
)

list(APPEND CURRENT_TARGET_SOURCES
        btree_storage.cpp
        range.cpp
)

add_library(rocketjoe_${CURRENT_TARGET}
        ${CURRENT_TARGET_HEADERS}
        ${CURRENT_TARGET_SOURCES}
)

add_library(rocketjoe::${CURRENT_TARGET} ALIAS rocketjoe_${CURRENT_TARGET})

set_property(TARGET rocketjoe_${CURRENT_TARGET} PROPERTY EXPORT_NAME ${CURRENT_TARGET})

target_link_libraries(
        rocketjoe_${CURRENT_TARGET} PRIVATE
        ${CONAN_LIBS}
        rocketjoe::document
)
