set(CURRENT_TARGET api)

list(APPEND CURRENT_TARGET_HEADERS
        http.hpp
        json_rpc.hpp
        transport_base.hpp
        websocket.hpp

)

list(APPEND CURRENT_TARGET_SOURCES
        http.cpp
        json_rpc.cpp
        transport_base.cpp
        websocket.cpp
)

include_directories(../thirdparty/json/include)

add_library(rocketjoe_${CURRENT_TARGET} STATIC
            ${CURRENT_TARGET_HEADERS}
            ${CURRENT_TARGET_SOURCES}
)



add_library(rocketjoe::${CURRENT_TARGET} ALIAS rocketjoe_${CURRENT_TARGET})

set_property(TARGET rocketjoe_${CURRENT_TARGET} PROPERTY EXPORT_NAME ${CURRENT_TARGET})

#target_link_libraries(
#        rocketjoe_${CURRENT_TARGET}
#)

target_include_directories(rocketjoe_${CURRENT_TARGET} PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/header" "${CMAKE_CURRENT_SOURCE_DIR}/../../")

install(TARGETS
        rocketjoe_${CURRENT_TARGET}

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)
