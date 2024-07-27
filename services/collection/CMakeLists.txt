project(collection)

set(${PROJECT_NAME}_SOURCES
        create_index.cpp
        create_index_utils.cpp
        executor.cpp
)

add_library(otterbrix_${PROJECT_NAME}
        ${${PROJECT_NAME}_SOURCES}
)


add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PUBLIC
        otterbrix::document
        otterbrix::types
        otterbrix::cursor
        otterbrix::session
        otterbrix::index
        otterbrix::logical_plan
        otterbrix::physical_plan_generator
        CONAN_PKG::spdlog
        CONAN_PKG::abseil
        CONAN_PKG::boost
)

target_include_directories(otterbrix_${PROJECT_NAME} PUBLIC
)

if (DEV_MODE)
    add_definitions(-DDEV_MODE)
    add_subdirectory(tests)
endif ()
