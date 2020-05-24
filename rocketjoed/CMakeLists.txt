set(CURRENT_TARGET rocketjoed)

set(${CURRENT_TARGET}_headers
        configuration.hpp
        init_service.hpp
)

set(${CURRENT_TARGET}_sources
        configuration.cpp
        init_service.cpp
        main.cpp
)

find_package(Python3 COMPONENTS Development)
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

configure_file("kernel.json.in" "${CMAKE_BINARY_DIR}/kernels/ipython/kernel.json")

if (DEV_MODE)

    find_program(PYTEST NAMES pytest)

    configure_file("test/test_jupyter_kernel.py" "${CMAKE_CURRENT_BINARY_DIR}/test/test_jupyter_kernel.py")

    add_test(NAME "test_jupyter_kernel.py" COMMAND "${PYTEST}" "${CMAKE_CURRENT_BINARY_DIR}/test/test_jupyter_kernel.py")

    set_tests_properties("test_jupyter_kernel.py" PROPERTIES ENVIRONMENT "JUPYTER_PATH=$JUPYTER_PATH:${CMAKE_BINARY_DIR}")

endif()

#install(TARGETS
#        ${CURRENT_TARGET}

#        RUNTIME DESTINATION bin
#        LIBRARY DESTINATION lib
#        ARCHIVE DESTINATION lib
#)
