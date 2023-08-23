project(memory_storage)

set(${PROJECT_NAME}_SOURCES
    memory_storage.cpp
    result.cpp
)

add_library(rocketjoe_${PROJECT_NAME}
    ${${PROJECT_NAME}_SOURCES}
)

add_library(rocketjoe::${PROJECT_NAME} ALIAS rocketjoe_${PROJECT_NAME})

set_property(TARGET rocketjoe_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
    rocketjoe_${PROJECT_NAME} PRIVATE
    rocketjoe::locks
    rocketjoe::session
    rocketjoe::collection
    CONAN_PKG::spdlog
)

target_include_directories(rocketjoe_${PROJECT_NAME} PUBLIC
)
