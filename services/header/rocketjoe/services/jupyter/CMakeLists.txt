set(CURRENT_TARGET jupyter)

list(APPEND "${CURRENT_TARGET}_HEADERS"
    interpreter.hpp
)

list(APPEND "${CURRENT_TARGET}_SOURCES"
    interpreter.cpp
)

find_package(Python3 REQUIRED COMPONENTS Development)

add_library("rocketjoe_${CURRENT_TARGET}" STATIC "${${CURRENT_TARGET}_HEADERS}"
            "${${CURRENT_TARGET}_SOURCES}")

add_library("rocketjoe::${CURRENT_TARGET}" ALIAS "rocketjoe_${CURRENT_TARGET}")

target_include_directories("rocketjoe_${CURRENT_TARGET}" PRIVATE
                           "${Python3_INCLUDE_DIRS}" PUBLIC
                           "${CMAKE_CURRENT_SOURCE_DIR}/../../..")

target_link_libraries("rocketjoe_${CURRENT_TARGET}" PRIVATE
                      "${Python3_LIBRARIES}" "${CONAN_LIBS}")

set_target_properties("rocketjoe_${CURRENT_TARGET}" PROPERTIES PUBLIC_HEADER
                      "${CURRENT_TARGET}_HEADERS")

configure_file("${CMAKE_CURRENT_SOURCE_DIR}/kernel.json.in"
               "${CMAKE_BINARY_DIR}/kernels/ipython/kernel.json")

if(DEV_MODE)
    find_program(PYTEST NAMES pytest-3 pytest py.test-3 py.test)

    configure_file("${CMAKE_CURRENT_SOURCE_DIR}/test_python_kernel.py"
                   "${CMAKE_CURRENT_BINARY_DIR}/test_python_kernel.py")

    add_test(NAME "test_python_kernel.py" COMMAND "${PYTEST}"
             "${CMAKE_CURRENT_BINARY_DIR}/test_python_kernel.py")

    set_tests_properties("test_python_kernel.py" PROPERTIES ENVIRONMENT
                         "JUPYTER_PATH=${CMAKE_BINARY_DIR}")
endif()
