set(CURRENT_TARGET python_engine)

list(APPEND ${CURRENT_TARGET}_HEADERS
        header/rocketjoe/services/python_engine/celery.hpp
        header/rocketjoe/services/python_engine/context_manager.hpp
        header/rocketjoe/services/python_engine/data_set_manager.hpp
        header/rocketjoe/services/python_engine/file_manager.hpp
        header/rocketjoe/services/python_engine/mapreduce.hpp
        header/rocketjoe/services/python_engine/python_engine.hpp
        header/rocketjoe/services/python_engine/python_sandbox.hpp
        header/rocketjoe/services/python_engine/file_system.hpp
)

list(APPEND ${CURRENT_TARGET}_SOURCES
        celery.cpp
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

target_include_directories(rocketjoe_${CURRENT_TARGET} PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/header" "${CMAKE_CURRENT_SOURCE_DIR}/../../")

install(TARGETS
        rocketjoe_${CURRENT_TARGET}

        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)
