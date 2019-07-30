set(CURRENT_TARGET network)

list(APPEND CURRENT_${TARGET_HEADERS}
        header/rocketjoe/network/query_context.hpp
)

list(APPEND CURRENT_${TARGET_HEADERS}
        query_context.cpp
)


add_library(rocketjoe_${CURRENT_TARGET} STATIC
            ${CURRENT_${TARGET_HEADERS}}
            ${CURRENT_${TARGET_HEADERS}}
)


add_library(rocketjoe::${CURRENT_TARGET} ALIAS rocketjoe_${CURRENT_TARGET})

set_property(TARGET rocketjoe_${CURRENT_TARGET} PROPERTY EXPORT_NAME ${CURRENT_TARGET})

target_link_libraries(
        rocketjoe_${CURRENT_TARGET}
        goblin-engineer
)

target_include_directories(
        rocketjoe_${CURRENT_TARGET}
        PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}/header/"
)

install(TARGETS
        rocketjoe_${CURRENT_TARGET}

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)
