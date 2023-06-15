project(logical_plan CXX)

set(SOURCE_${PROJECT_NAME}
        node.cpp
        node_aggregate.cpp
        node_delete.cpp
        node_group.cpp
        node_insert.cpp
        node_limit.cpp
        node_match.cpp
        node_sort.cpp
)

add_library(rocketjoe_${PROJECT_NAME}
        ${SOURCE_${PROJECT_NAME}}
)

add_library(rocketjoe::${PROJECT_NAME} ALIAS rocketjoe_${PROJECT_NAME})

set_property(TARGET rocketjoe_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        rocketjoe_${PROJECT_NAME} PRIVATE
        rocketjoe::ql
        CONAN_PKG::boost
)

target_include_directories(
        rocketjoe_${PROJECT_NAME}
        PUBLIC
)
