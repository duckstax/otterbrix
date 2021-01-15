set(CURRENT_TARGET python_sandbox)

list(APPEND ${CURRENT_TARGET}_HEADERS
        detail/context.hpp
        detail/context_manager.hpp
        detail/data_set.hpp
        detail/file_manager.hpp
        detail/file_system.hpp
        detail/forward.hpp
        detail/mapreduce.hpp
        python.hpp
)

list(APPEND ${CURRENT_TARGET}_SOURCES
        source/detail/context.cpp
        source/detail/context_manager.cpp
        source/detail/data_set.cpp
        source/detail/file_manager.cpp
        source/detail/file_system.cpp
        source/detail/mapreduce.cpp
        source/python.cpp
)

add_library(rocketjoe_${CURRENT_TARGET}
            ${${CURRENT_TARGET}_HEADERS}
            ${${CURRENT_TARGET}_SOURCES}
)

add_library(rocketjoe::${CURRENT_TARGET} ALIAS rocketjoe_${CURRENT_TARGET})

set_property(TARGET rocketjoe_${CURRENT_TARGET} PROPERTY EXPORT_NAME ${CURRENT_TARGET})

target_link_libraries(
        rocketjoe_${CURRENT_TARGET} PRIVATE
        rocketjoe::log
        pybind11::embed
)

target_include_directories(rocketjoe_${CURRENT_TARGET} PUBLIC "")

if (DEV_MODE)
    add_subdirectory(tests)
endif()
