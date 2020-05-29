set(CURRENT_TARGET configuration)

list(APPEND CURRENT_${TARGET_HEADERS}
        configuration.hpp
)

#list(APPEND CURRENT_${TARGET_HEADERS}
#)


add_library(rocketjoe_${CURRENT_TARGET} INTERFACE
#            ${CURRENT_${TARGET_HEADERS}}
#            ${CURRENT_${TARGET_HEADERS}}
)


add_library(rocketjoe::${CURRENT_TARGET} ALIAS rocketjoe_${CURRENT_TARGET})

set_property(TARGET rocketjoe_${CURRENT_TARGET} PROPERTY EXPORT_NAME ${CURRENT_TARGET})

target_link_libraries(
        rocketjoe_${CURRENT_TARGET}
        INTERFACE
        #PRIVATE
        #${Boost_LIBRARIES}
        #${CONAN_LIBS}
)

target_include_directories(
        rocketjoe_${CURRENT_TARGET}
        INTERFACE
        "../.."
)


install(TARGETS
        rocketjoe_${CURRENT_TARGET}

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)
