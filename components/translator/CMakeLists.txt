project(translator CXX)

set(SOURCE_${PROJECT_NAME}
        ql_translator.cpp
)

add_library(ottergon_${PROJECT_NAME}
        ${SOURCE_${PROJECT_NAME}}
)

add_library(ottergon::${PROJECT_NAME} ALIAS ottergon_${PROJECT_NAME})

set_property(TARGET ottergon_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        ottergon_${PROJECT_NAME} PRIVATE
        ottergon::logical_plan
        ottergon::ql
)

target_include_directories(
        ottergon_${PROJECT_NAME}
        PUBLIC
)

if (DEV_MODE)
    add_subdirectory(test)
endif ()