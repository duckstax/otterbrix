PROJECT(network_service)

include_directories(${ROOT_PATH}/components)

set(${PROJECT_NAME}_SOURCES
        context.cpp
        detect_session.cpp
        dto.cpp
        http_client.cpp
        http_session.cpp
        network_service.cpp
        plain_http_session.cpp
        plain_websocket_session.cpp
        ssl_http_session.cpp
        ssl_websocket_session.cpp
        websocket_session.cpp
)

set(${PROJECT_NAME}_HEADER
        context.hpp
        detect_session.hpp
        dto.hpp
        http_client.hpp
        http_session.hpp
        network_service.hpp
        plain_http_session.hpp
        plain_websocket_session.hpp
        server_certificate.hpp
        ssl_http_session.hpp
        ssl_websocket_session.hpp
        websocket_session.hpp
)

if(USE_TRACY)
    include_directories(${TRACY_PATH})
    set(${PROJECT_NAME}_SOURCES ${${PROJECT_NAME}_SOURCES}
        ${TRACY_PATH}/TracyClient.cpp
    )
endif()

add_library(
        ${PROJECT_NAME}
        ${${PROJECT_NAME}_SOURCES}
        ${${PROJECT_NAME}_HEADER}
)

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        rocketjoe::log
        ${CONAN_LIBS}
)