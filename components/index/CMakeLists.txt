project(index)

set(${PROJECT_NAME}_SOURCES
        hash_index.cpp
        index.cpp
        index_engine.cpp
        single_field_index.cpp
)

add_library(ottergon_${PROJECT_NAME}
        ${${PROJECT_NAME}_SOURCES}
)


add_library(ottergon::${PROJECT_NAME} ALIAS ottergon_${PROJECT_NAME})

set_property(TARGET ottergon_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        ottergon_${PROJECT_NAME} PUBLIC
        ottergon::document
        ottergon::cursor
        ottergon::ql
        ottergon::pipeline
        dl
        CONAN_PKG::boost
)

target_include_directories(
        ottergon_${PROJECT_NAME}
        PUBLIC

)

if (DEV_MODE)
    add_subdirectory(test)
endif ()
