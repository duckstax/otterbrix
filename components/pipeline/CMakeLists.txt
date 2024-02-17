project(pipeline)

set(${PROJECT_NAME}_SOURCES
        context.cpp
)

add_library(otterbrix_${PROJECT_NAME}
        ${${PROJECT_NAME}_SOURCES}
)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PUBLIC
        otterbrix::ql
        otterbrix::session
)

target_include_directories(otterbrix_${PROJECT_NAME} PUBLIC
)
