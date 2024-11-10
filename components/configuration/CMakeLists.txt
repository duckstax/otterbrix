set(CURRENT_TARGET configuration)

list(APPEND CURRENT_${TARGET_HEADERS}
        configuration.hpp
)

#list(APPEND CURRENT_${TARGET_HEADERS}
#)


add_library(otterbrix_${CURRENT_TARGET} INTERFACE
#            ${CURRENT_${TARGET_HEADERS}}
#            ${CURRENT_${TARGET_HEADERS}}
)


add_library(otterbrix::${CURRENT_TARGET} ALIAS otterbrix_${CURRENT_TARGET})

set_property(TARGET otterbrix_${CURRENT_TARGET} PROPERTY EXPORT_NAME ${CURRENT_TARGET})

target_link_libraries(
        otterbrix_${CURRENT_TARGET}
        INTERFACE
        #PRIVATE
        #${Boost_LIBRARIES}
)

target_include_directories(
        otterbrix_${CURRENT_TARGET}
        INTERFACE
)


install(TARGETS
        otterbrix_${CURRENT_TARGET}

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)
