set(CURRENT_TARGET rocketjoe_worker)

set(${CURRENT_TARGET}_headers
        configuration.hpp
        init_service.hpp
)

set(${CURRENT_TARGET}_sources
        configuration.cpp
        init_service.cpp
        main.cpp
)

include_directories(${Python3_INCLUDE_DIRS})

add_executable(${CURRENT_TARGET} ${${CURRENT_TARGET}_sources} ${${CURRENT_TARGET}_headers})

target_link_libraries(
        ${CURRENT_TARGET} PRIVATE
        ${CONAN_LIBS}
        rocketjoe::configuration
        rocketjoe::python_sandbox
        rocketjoe::log
        rocketjoe::process_pool
        dl
)



if (DEV_MODE)

    find_program(PYTEST NAMES pytest)
    configure_file("test/test_rocketjoe.py" "${CMAKE_CURRENT_BINARY_DIR}/test/test_rocketjoe.py")
    add_test(NAME "test_rocketjoe.py" COMMAND "python3" "${CMAKE_CURRENT_BINARY_DIR}/test/test_rocketjoe.py")

endif ()

#install(TARGETS
#        ${CURRENT_TARGET}

#        RUNTIME DESTINATION bin
#        LIBRARY DESTINATION lib
#        ARCHIVE DESTINATION lib
#)
