set(CURRENT_TARGET ssh_forwarder)

list(APPEND CURRENT_TARGET_HEADERS
    ssh_forwarder.hpp
)

list(APPEND CURRENT_TARGET_SOURCES
    ssh_forwarder.cpp
)

add_library(rocketjoe_${CURRENT_TARGET}
    ${CURRENT_TARGET_HEADERS}
    ${CURRENT_TARGET_SOURCES}
)

add_library(rocketjoe::${CURRENT_TARGET} ALIAS rocketjoe_${CURRENT_TARGET})

set_property(TARGET rocketjoe_${CURRENT_TARGET} PROPERTY EXPORT_NAME ${CURRENT_TARGET})

target_link_libraries(
    rocketjoe_${CURRENT_TARGET} PRIVATE
    ${CONAN_LIBS}
)

install(TARGETS
    rocketjoe_${CURRENT_TARGET}

    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)
