project(expressions CXX)

set(SOURCE_${PROJECT_NAME}
        expression.cpp
        compare_expression.cpp
        aggregate_expression.cpp
        scalar_expression.cpp
        sort_expression.cpp
)

add_library(ottergon_${PROJECT_NAME}
        ${SOURCE_${PROJECT_NAME}}
)

add_library(ottergon::${PROJECT_NAME} ALIAS ottergon_${PROJECT_NAME})

set_property(TARGET ottergon_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        ottergon_${PROJECT_NAME} PRIVATE
        CONAN_PKG::boost
)

target_include_directories(
        ottergon_${PROJECT_NAME}
        PUBLIC
)

if (DEV_MODE)
    add_subdirectory(test)
endif ()