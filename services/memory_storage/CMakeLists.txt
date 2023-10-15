project(memory_storage)

set(${PROJECT_NAME}_SOURCES
    memory_storage.cpp
)

add_library(rocketjoe_${PROJECT_NAME}
    ${${PROJECT_NAME}_SOURCES}
)

add_library(rocketjoe::${PROJECT_NAME} ALIAS rocketjoe_${PROJECT_NAME})

set_property(TARGET rocketjoe_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
    rocketjoe_${PROJECT_NAME} PRIVATE
    rocketjoe::collection
    rocketjoe::disk
    rocketjoe::locks
    rocketjoe::result
    rocketjoe::session
    CONAN_PKG::spdlog
    actor #CONAN_PKG::actor-zeta
)

target_include_directories(rocketjoe_${PROJECT_NAME} PUBLIC
)
