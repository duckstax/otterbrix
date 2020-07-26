set(CURRENT_TARGET interactive_python_interpreter)

set( ${CURRENT_TARGET}_HEADERS
        interactive_python.hpp
)

set(${CURRENT_TARGET}_SOURCES
        interactive_python.cpp
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
        rocketjoe::python_sandbox
        rocketjoe::log
        goblin-engineer
)

target_include_directories(rocketjoe_${CURRENT_TARGET} PUBLIC "")

if (DEV_MODE)
#    add_subdirectory(tests)
endif()
