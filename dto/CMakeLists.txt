set(CURRENT_TARGET dto)

list(APPEND ${CURRENT_TARGET}_HEADERS
        header/rocketjoe/dto/json_rpc.hpp
)

list(APPEND ${CURRENT_TARGET}_SOURCES
        json_rpc.cpp
)

include_directories(../thirdparty/json/include)

add_library(rocketjoe_${CURRENT_TARGET} STATIC
        ${${CURRENT_TARGET}_HEADERS}
        ${${CURRENT_TARGET}_SOURCES}
)

add_library(rocketjoe::${CURRENT_TARGET} ALIAS rocketjoe_${CURRENT_TARGET})

set_property(TARGET rocketjoe_${CURRENT_TARGET} PROPERTY EXPORT_NAME ${CURRENT_TARGET})

#target_link_libraries(
#        rocketjoe_${CURRENT_TARGET}
#)

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
