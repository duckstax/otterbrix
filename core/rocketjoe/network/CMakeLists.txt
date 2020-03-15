set(CURRENT_TARGET network)

list(APPEND CURRENT_${TARGET_HEADERS}
        http_session.hpp
        network.hpp
        router.hpp
        server.hpp
        service_router.hpp
        websocket_session.hpp
)

list(APPEND CURRENT_${TARGET_HEADERS}
        http_session.cpp
        network.cpp
        server.cpp
        service_router.cpp
        websocket_session.cpp
)


add_library(rocketjoe_${CURRENT_TARGET} STATIC
            ${CURRENT_${TARGET_HEADERS}}
            ${CURRENT_${TARGET_HEADERS}}
)


add_library(rocketjoe::${CURRENT_TARGET} ALIAS rocketjoe_${CURRENT_TARGET})

set_property(TARGET rocketjoe_${CURRENT_TARGET} PROPERTY EXPORT_NAME ${CURRENT_TARGET})

target_link_libraries(
        rocketjoe_${CURRENT_TARGET} PRIVATE
        ${Boost_LIBRARIES}
        ${CONAN_LIBS}
)

target_include_directories(
        rocketjoe_${CURRENT_TARGET}
        PUBLIC
        "../../../"
)

install(TARGETS
        rocketjoe_${CURRENT_TARGET}

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)
