set(CURRENT_TARGET http)

list(APPEND CURRENT_TARGET_HEADERS
        header/rocketjoe/http/context.hpp
        header/rocketjoe/http/server.hpp
        header/rocketjoe/http/session.hpp
        header/rocketjoe/http/router.hpp
        header/rocketjoe/http/websocket_session.hpp
)

list(APPEND CURRENT_TARGET_SOURCES
        session.cpp
        server.cpp
        websocket_session.cpp
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
