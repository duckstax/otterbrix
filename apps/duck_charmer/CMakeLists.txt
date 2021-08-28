cmake_minimum_required(VERSION 3.15)

project(duck_charmer)


set(${PROJECT_NAME}_headers
)

set(${PROJECT_NAME}_sources
        main.cpp
)

include_directories(${PROJECT_SOURCE_DIR}

)

pybind11_add_module(${PROJECT_NAME}  ${${PROJECT_NAME}_sources} ${${PROJECT_NAME}_headers} )

target_link_libraries(
        ${PROJECT_NAME}
        PRIVATE
        ${CONAN_LIBS}
)

set_target_properties(${PROJECT_NAME} PROPERTIES CXX_VISIBILITY_PRESET "hidden")
