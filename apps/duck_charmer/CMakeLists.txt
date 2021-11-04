cmake_minimum_required(VERSION 3.15)

project(duck_charmer)


set(${PROJECT_NAME}_headers
        convert.hpp
        dispatcher.hpp
        spaces.hpp
        wrapper_client.hpp
        wrapper_collection.hpp
        wrapper_database.hpp
        wrapper_document.hpp
        wrapper_cursor.hpp
)

set(${PROJECT_NAME}_sources
        convert.cpp
        dispatcher.cpp
        main.cpp
        spaces.cpp
        wrapper_client.cpp
        wrapper_collection.cpp
        wrapper_database.cpp
        wrapper_document.cpp
        wrapper_cursor.cpp
)

include_directories(${PROJECT_SOURCE_DIR}

)

pybind11_add_module(${PROJECT_NAME} ${${PROJECT_NAME}_sources} ${${PROJECT_NAME}_headers})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        ${CONAN_LIBS}
        rocketjoe::log
        rocketjoe::storage
        rocketjoe::cursor
        rocketjoe::session
)

set_target_properties(${PROJECT_NAME} PROPERTIES CXX_VISIBILITY_PRESET "hidden")
