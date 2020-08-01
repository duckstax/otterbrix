set(CURRENT_TARGET jupyter)

set(${CURRENT_TARGET}_HEADERS
        jupyter.hpp
)

set(${CURRENT_TARGET}_SOURCES
        jupyter.cpp
)

add_library(rocketjoe_${CURRENT_TARGET} STATIC
        ${${CURRENT_TARGET}_HEADERS}
        ${${CURRENT_TARGET}_SOURCES}
)


add_library(rocketjoe::${CURRENT_TARGET} ALIAS rocketjoe_${CURRENT_TARGET})

set_property(TARGET rocketjoe_${CURRENT_TARGET} PROPERTY EXPORT_NAME ${CURRENT_TARGET})

target_link_libraries(
        rocketjoe_${CURRENT_TARGET} PRIVATE
        rocketjoe::log
        rocketjoe::buffer
        ${CONAN_LIBS}
)

target_include_directories(rocketjoe_${CURRENT_TARGET} PUBLIC "")

if (DEV_MODE)
#    add_subdirectory(tests)
endif ()
