set(CURRENT_TARGET python_sandbox)

list(APPEND ${CURRENT_TARGET}_HEADERS
        detail/jupyter/display_hook.hpp
        detail/jupyter/shell_display_hook.hpp
        detail/jupyter/display_publisher.hpp
        detail/jupyter/pykernel.hpp
        detail/jupyter/session.hpp
        detail/jupyter/shell.hpp
        detail/jupyter/zmq_ostream.hpp
        detail/jupyter/zmq_socket_shared.hpp

        detail/celery.hpp
        detail/context.hpp
        detail/context_manager.hpp
        detail/data_set.hpp
        detail/file_manager.hpp
        detail/file_system.hpp
        detail/forward.hpp
        detail/jupyter.hpp
        detail/mapreduce.hpp

        python_sandbox.hpp
)

list(APPEND ${CURRENT_TARGET}_SOURCES
        source/detail/jupyter/display_hook.cpp
        source/detail/jupyter/shell_display_hook.cpp
        source/detail/jupyter/display_publisher.cpp
        source/detail/jupyter/pykernel.cpp
        source/detail/jupyter/session.cpp
        source/detail/jupyter/shell.cpp
        source/detail/jupyter/zmq_ostream.cpp
        source/detail/jupyter/zmq_socket_shared.cpp

        source/detail/celery.cpp
        source/detail/context.cpp
        source/detail/context_manager.cpp
        source/detail/data_set.cpp
        source/detail/file_manager.cpp
        source/detail/file_system.cpp
        source/detail/jupyter.cpp

        kernel.json.in
        source/detail/mapreduce.cpp
        source/python_sandbox.cpp
)

add_library(rocketjoe_${CURRENT_TARGET} STATIC
            ${${CURRENT_TARGET}_HEADERS}
            ${${CURRENT_TARGET}_SOURCES}
)

find_package(Python3 COMPONENTS Development)
include_directories(${Python3_INCLUDE_DIRS})
find_package(pybind11 REQUIRED)

add_library(rocketjoe::${CURRENT_TARGET} ALIAS rocketjoe_${CURRENT_TARGET})

set_property(TARGET rocketjoe_${CURRENT_TARGET} PROPERTY EXPORT_NAME ${CURRENT_TARGET})

target_link_libraries(
        rocketjoe_${CURRENT_TARGET} PRIVATE
        rocketjoe::log
        goblin-engineer
        pybind11::embed
)

target_include_directories(rocketjoe_${CURRENT_TARGET} PUBLIC "")

configure_file("kernel.json.in" "${CMAKE_BINARY_DIR}/kernels/ipython/kernel.json")

if (DEV_MODE)
    add_subdirectory(tests)

    find_program(PYTEST NAMES pytest )

    configure_file("test_python_kernel.py"
            "${CMAKE_CURRENT_BINARY_DIR}/test_python_kernel.py")

    add_test(NAME "test_python_kernel.py" COMMAND "${PYTEST}"
            "${CMAKE_CURRENT_BINARY_DIR}/test_python_kernel.py")

    set_tests_properties("test_python_kernel.py" PROPERTIES ENVIRONMENT
            "JUPYTER_PATH=${CMAKE_BINARY_DIR}")

endif()
