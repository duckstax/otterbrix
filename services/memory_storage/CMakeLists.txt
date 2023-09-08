project(memory_storage)

set(${PROJECT_NAME}_SOURCES
    memory_storage.cpp
)

add_library(ottergon_${PROJECT_NAME}
    ${${PROJECT_NAME}_SOURCES}
)

add_library(ottergon::${PROJECT_NAME} ALIAS ottergon_${PROJECT_NAME})

set_property(TARGET ottergon_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
    ottergon_${PROJECT_NAME} PRIVATE
    ottergon::collection
    ottergon::disk
    ottergon::locks
    ottergon::result
    ottergon::session
    CONAN_PKG::spdlog
)

target_include_directories(ottergon_${PROJECT_NAME} PUBLIC
)
