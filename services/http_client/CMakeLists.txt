set(CURRENT_TARGET http_client)

list(APPEND CURRENT_TARGET_HEADERS
header/rocketjoe/services/http_client/http_client.hpp
)

list(APPEND CURRENT_TARGET_SOURCES
        http_client.cpp
        client.cpp
)


add_library(rocketjoe_${CURRENT_TARGET} STATIC
            ${CURRENT_TARGET_HEADERS}
            ${CURRENT_TARGET_SOURCES}
)


add_library(rocketjoe::${CURRENT_TARGET} ALIAS rocketjoe_${CURRENT_TARGET})

set_property(TARGET rocketjoe_${CURRENT_TARGET} PROPERTY EXPORT_NAME ${CURRENT_TARGET})

target_link_libraries(
        rocketjoe_${CURRENT_TARGET}
        goblin-engineer
        rocketjoe::network
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
