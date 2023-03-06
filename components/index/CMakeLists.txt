project(index)

set(${PROJECT_NAME}_SOURCES
        hash_index.cpp
        index.cpp
        index_engine.cpp
        single_field_index.cpp
        #disk_single_field_index.cpp
)

add_library(rocketjoe_${PROJECT_NAME}
        ${${PROJECT_NAME}_SOURCES}
)


add_library(rocketjoe::${PROJECT_NAME} ALIAS rocketjoe_${PROJECT_NAME})

set_property(TARGET rocketjoe_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        rocketjoe_${PROJECT_NAME} PUBLIC
        rocketjoe::document
        rocketjoe::cursor
        rocketjoe::ql
        dl
        CONAN_PKG::boost
)

target_include_directories(
        rocketjoe_${PROJECT_NAME}
        PUBLIC

)

if (DEV_MODE)
    add_subdirectory(test)
endif ()
