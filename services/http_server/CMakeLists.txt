set(CURRENT_TARGET http_server)

list(APPEND CURRENT_TARGET_HEADERS
        header/rocketjoe/services/http_server/http_server.hpp
        header/rocketjoe/services/http_server/http_session.hpp
        header/rocketjoe/services/http_server/websocket_session.hpp
        header/rocketjoe/services/http_server/listener.hpp

)

list(APPEND CURRENT_TARGET_SOURCES
        http_session.cpp
        listener.cpp
        http_server.cpp
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
        ${CMAKE_THREAD_LIBS_INIT}
        rocketjoe::api
        goblin-engineer
)

target_include_directories(
        rocketjoe_${CURRENT_TARGET}
        PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}/header"
        "${CMAKE_CURRENT_SOURCE_DIR}/../../"
)

install(TARGETS
        rocketjoe_${CURRENT_TARGET}

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)
