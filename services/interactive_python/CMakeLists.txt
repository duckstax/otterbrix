set(CURRENT_TARGET interactive_python)

set( ${CURRENT_TARGET}_HEADERS
        jupyter/display_hook.hpp
        jupyter/display_publisher.hpp
        jupyter/hmac.hpp
        jupyter/pykernel.hpp
        jupyter/session.hpp
        jupyter/shell.hpp
        jupyter/shell_display_hook.hpp
        jupyter/socket_manager.hpp
        jupyter/zmq_ostream.hpp
        add_jupyter.hpp
        interactive_python.hpp
)

set(${CURRENT_TARGET}_SOURCES
        jupyter/display_hook.cpp
        jupyter/display_publisher.cpp
        jupyter/hmac.cpp
        jupyter/pykernel.cpp
        jupyter/session.cpp
        jupyter/shell.cpp
        jupyter/shell_display_hook.cpp
        jupyter/socket_manager.cpp
        jupyter/zmq_ostream.cpp
        add_jupyter.cpp
        interactive_python.cpp
)

add_library(rocketjoe_${CURRENT_TARGET}
            ${${CURRENT_TARGET}_HEADERS}
            ${${CURRENT_TARGET}_SOURCES}
)

find_package(Python3 COMPONENTS Interpreter Development)
include_directories(${Python3_INCLUDE_DIRS})
find_package(pybind11 REQUIRED)

add_library(rocketjoe::${CURRENT_TARGET} ALIAS rocketjoe_${CURRENT_TARGET})

set_property(TARGET rocketjoe_${CURRENT_TARGET} PROPERTY EXPORT_NAME ${CURRENT_TARGET})

if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    target_link_libraries(
            rocketjoe_${CURRENT_TARGET} PRIVATE
            iconv
    )
endif()

target_link_libraries(
        rocketjoe_${CURRENT_TARGET} PRIVATE
        ${CONAN_LIBS}
        rocketjoe::python_sandbox
        pybind11::embed
        rocketjoe::log
        rocketjoe::buffer
)

target_include_directories(rocketjoe_${CURRENT_TARGET} PUBLIC "")

if (DEV_MODE)
    add_subdirectory(tests)
endif()
