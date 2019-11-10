set(CURRENT_TARGET python_engine)

list(APPEND ${CURRENT_TARGET}_HEADERS
        detail/celery.hpp
        detail/context_manager.hpp
        detail/data_set_manager.hpp
        detail/file_manager.hpp
        detail/mapreduce.hpp
        python_engine.hpp
        python_sandbox.hpp
        detail/file_system.hpp
)

list(APPEND ${CURRENT_TARGET}_SOURCES
        celery.cpp
        context_manager.cpp
        data_set_manager.cpp
        file_manager.cpp
        file_system.cpp
        mapreduce.cpp
        python_engine.cpp
        python_sandbox.cpp
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
        goblin-engineer
        PRIVATE pybind11::embed
)

target_include_directories(rocketjoe_${CURRENT_TARGET} PUBLIC "../../../../python_engine/header" "../../../../..")

install(TARGETS
        rocketjoe_${CURRENT_TARGET}

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)

if (DEV_MODE)
    add_subdirectory(tests)
endif()