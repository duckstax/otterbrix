project(collection)

set( ${PROJECT_NAME}_HEADERS
        collection.hpp
        result.hpp
        sort.hpp
)

set(${PROJECT_NAME}_SOURCES
        collection.cpp
        result.cpp
        sort.cpp
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
        rocketjoe::cursor
        rocketjoe::session
        rocketjoe::parser
)

target_include_directories(rocketjoe_${PROJECT_NAME} PUBLIC
)

if (DEV_MODE)
    add_definitions(-DDEV_MODE)
    add_subdirectory(tests)
endif()