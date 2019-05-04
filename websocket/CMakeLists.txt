set(CURRENT_TARGET ws)

list(APPEND CURRENT_TARGET_HEADERS
        header/rocketjoe/data_provider/ws/ws_server.hpp
        header/rocketjoe/data_provider/ws/ws_session.hpp
        header/rocketjoe/data_provider/ws/ws_listener.hpp
)

list(APPEND CURRENT_TARGET_SOURCES
        ws_listener.cpp
        ws_server.cpp
        ws_session.cpp
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
        "header"
        ".."
)

install(TARGETS
        rocketjoe_${CURRENT_TARGET}

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)
