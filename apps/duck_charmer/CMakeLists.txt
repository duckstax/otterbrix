cmake_minimum_required(VERSION 3.15)

project(duck_charmer)

set(${PROJECT_NAME}_headers
        convert.hpp
        spaces.hpp
        wrapper_client.hpp
        wrapper_collection.hpp
        wrapper_database.hpp
        wrapper_cursor.hpp
        wrapper_dispatcher.hpp
        wrapper_document.hpp
        wrapper_result.hpp
)

set(${PROJECT_NAME}_sources
        convert.cpp
        main.cpp
        spaces.cpp
        wrapper_client.cpp
        wrapper_collection.cpp
        wrapper_database.cpp
        wrapper_cursor.cpp
        wrapper_dispatcher.cpp
        wrapper_document.cpp
        wrapper_result.cpp
)

set(ROCKETJOE_LIBS
        rocketjoe::log
        rocketjoe::database
        rocketjoe::collection
        rocketjoe::cursor
        rocketjoe::session
        rocketjoe::document
        rocketjoe::dispatcher
        rocketjoe::parser
        rocketjoe::wal
        )

include_directories(
        ${PROJECT_SOURCE_DIR}
)

pybind11_add_module(${PROJECT_NAME} ${${PROJECT_NAME}_sources} ${${PROJECT_NAME}_headers})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        ${CONAN_LIBS}
        ${ROCKETJOE_LIBS}
)

set_target_properties(${PROJECT_NAME} PROPERTIES CXX_VISIBILITY_PRESET "hidden")

if (DEV_MODE)
    file(GLOB PY_TESTS
        "py_test/*.py"
    )
    file(COPY ${PY_TESTS} DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
endif()

if (DEV_MODE)
    set(${PROJECT_NAME}_headers
            spaces.hpp
            wrapper_dispatcher.hpp
            )

    set(${PROJECT_NAME}_sources
            spaces.cpp
            wrapper_dispatcher.cpp
            )

    add_library(rocketjoe_${PROJECT_NAME}
            ${${PROJECT_NAME}_headers}
            ${${PROJECT_NAME}_sources}
            )

    add_library(rocketjoe::${PROJECT_NAME} ALIAS rocketjoe_${PROJECT_NAME})

    set_property(TARGET rocketjoe_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

    target_link_libraries(
            rocketjoe_${PROJECT_NAME} PRIVATE
            ${CONAN_LIBS}
            ${ROCKETJOE_LIBS}
    )

    add_subdirectory(test)
endif()