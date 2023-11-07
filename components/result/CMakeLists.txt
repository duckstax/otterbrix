project(result)

set(${PROJECT_NAME}_SOURCES
    result.cpp
)

add_library(ottergon_${PROJECT_NAME}
    ${${PROJECT_NAME}_SOURCES}
)

add_library(ottergon::${PROJECT_NAME} ALIAS ottergon_${PROJECT_NAME})

set_property(TARGET ottergon_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
    ottergon_${PROJECT_NAME} PRIVATE
    ottergon::ql
)

target_include_directories(ottergon_${PROJECT_NAME} PUBLIC
)
