set(CURRENT_TARGET cache)

list(APPEND ${CURRENT_TARGET}_HEADERS
        header/rocketjoe/cache/cache_commands.hpp
        header/rocketjoe/cache/cache_element.hpp

)

list(APPEND ${CURRENT_TARGET}_SOURCES
        cache_commands.cpp

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

target_include_directories(rocketjoe_${CURRENT_TARGET} PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/header/")

install(TARGETS
        rocketjoe_${CURRENT_TARGET}

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)
