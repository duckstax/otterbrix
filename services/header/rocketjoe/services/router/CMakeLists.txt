set(CURRENT_TARGET router)

list(APPEND CURRENT_TARGET_HEADERS
        router.hpp
        service_router.hpp

)

list(APPEND CURRENT_TARGET_SOURCES
        service_router.cpp
)

add_library(rocketjoe_${CURRENT_TARGET} STATIC
            ${CURRENT_TARGET_HEADERS}
            ${CURRENT_TARGET_SOURCES}
)

add_library(rocketjoe::${CURRENT_TARGET} ALIAS rocketjoe_${CURRENT_TARGET})

set_property(TARGET rocketjoe_${CURRENT_TARGET} PROPERTY EXPORT_NAME ${CURRENT_TARGET})

target_link_libraries(
        rocketjoe_${CURRENT_TARGET} #PRIVATE # not work
        rocketjoe::network
        rocketjoe::http_server
        goblin-engineer
)

target_include_directories(
        rocketjoe_${CURRENT_TARGET}
        PUBLIC
        "../../../../router/header"
)

install(TARGETS
        rocketjoe_${CURRENT_TARGET}

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)


if (DEV_MODE)
    add_subdirectory(tests)
endif()