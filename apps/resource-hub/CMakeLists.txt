set(CURRENT_TARGET resource-hub)

set(${CURRENT_TARGET}_headers

)

set(${CURRENT_TARGET}_sources
        main.cpp
)

add_executable(${CURRENT_TARGET} ${${CURRENT_TARGET}_sources} ${${CURRENT_TARGET}_headers})

target_link_libraries(
        ${CURRENT_TARGET} PRIVATE
        ${CONAN_LIBS}
        dl
)

if (DEV_MODE)

endif()

#install(TARGETS
#        ${CURRENT_TARGET}

#        RUNTIME DESTINATION bin
#        LIBRARY DESTINATION lib
#        ARCHIVE DESTINATION lib
#)
