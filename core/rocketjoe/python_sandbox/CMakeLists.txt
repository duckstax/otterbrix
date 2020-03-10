set(CURRENT_TARGET python_sandbox)

list(APPEND ${CURRENT_TARGET}_HEADERS
        detail/jupyter/interpreter.hpp
        detail/celery.hpp
        detail/context.hpp
        detail/context_manager.hpp
        detail/file_manager.hpp
        detail/mapreduce.hpp
        detail/context.hpp
        detail/data_set.hpp

        python_sandbox.hpp
        detail/file_system.hpp
)

list(APPEND ${CURRENT_TARGET}_SOURCES
        detail/jupyter/interpreter.cpp
        celery.cpp
        context_manager.cpp
        file_manager.cpp
        file_system.cpp
        mapreduce.cpp
        python_sandbox.cpp
        context.cpp
        data_set.cpp
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
        rocketjoe::network
        rocketjoe::http_server
        rocketjoe::log
        goblin-engineer
        pybind11::embed
)

target_include_directories(rocketjoe_${CURRENT_TARGET} PUBLIC "")

configure_file("kernel.json.in" "${CMAKE_BINARY_DIR}/kernels/ipython/kernel.json")

if (DEV_MODE)
    add_subdirectory(tests)

    find_program(PYTEST NAMES pytest-3 pytest py.test-3 py.test)

    configure_file("test_python_kernel.py"
            "${CMAKE_CURRENT_BINARY_DIR}/test_python_kernel.py")

    add_test(NAME "../../test_python_kernel.py" COMMAND "${PYTEST}"
            "${CMAKE_CURRENT_BINARY_DIR}/test_python_kernel.py")

    set_tests_properties("../../test_python_kernel.py" PROPERTIES ENVIRONMENT
            "JUPYTER_PATH=${CMAKE_BINARY_DIR}")

endif()
