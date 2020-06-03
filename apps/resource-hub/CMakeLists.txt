set(CURRENT_TARGET resource-hub)

set(${CURRENT_TARGET}_headers
    init_service.hpp
)

set(${CURRENT_TARGET}_sources
        init_service.cpp
        main.cpp
)

add_executable(${CURRENT_TARGET} ${${CURRENT_TARGET}_sources} ${${CURRENT_TARGET}_headers})

target_link_libraries(
        ${CURRENT_TARGET} PRIVATE
        rocketjoe::log
        rocketjoe::zmq-hub
        rocketjoe::storage
        rocketjoe::configuration
        rocketjoe::process_pool
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
