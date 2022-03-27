project(serialize)

list(APPEND ${PROJECT_NAME}_HEADERS
        serialize.hpp
)

list(APPEND ${PROJECT_NAME}_SOURCES
        serialize.cpp
)

add_library(rocketjoe_${PROJECT_NAME}
        ${${PROJECT_NAME}_HEADERS}
        ${${PROJECT_NAME}_SOURCES}
)

add_library(rocketjoe::${PROJECT_NAME} ALIAS rocketjoe_${PROJECT_NAME})

set_property(TARGET rocketjoe_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        rocketjoe_${PROJECT_NAME} PRIVATE
        ${CONAN_LIBS}
        rocketjoe::document
)

if (DEV_MODE)
    add_subdirectory(tests)
endif()