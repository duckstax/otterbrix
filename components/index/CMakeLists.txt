project(index)

set(${PROJECT_NAME}_HEADERS

)

set(${PROJECT_NAME}_SOURCES
        composite_field_index.cpp
        index.cpp
        multi_field_index.cpp
        single_field_index.cpp
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
        dl
)

target_include_directories(
        rocketjoe_${PROJECT_NAME}
        PUBLIC

)

if (DEV_MODE)
    add_subdirectory(test)
endif ()
