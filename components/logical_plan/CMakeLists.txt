project(logical_plan CXX)

set(SOURCE_${PROJECT_NAME}
        node.cpp
        node_aggregate.cpp
        node_create_collection.cpp
        node_create_database.cpp
        node_delete.cpp
        node_drop_collection.cpp
        node_drop_database.cpp
        node_group.cpp
        node_insert.cpp
        node_limit.cpp
        node_match.cpp
        node_sort.cpp
        node_update.cpp
)

add_library(ottergon_${PROJECT_NAME}
        ${SOURCE_${PROJECT_NAME}}
)

add_library(ottergon::${PROJECT_NAME} ALIAS ottergon_${PROJECT_NAME})

set_property(TARGET ottergon_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        ottergon_${PROJECT_NAME} PRIVATE
        ottergon::ql
        CONAN_PKG::boost
)

target_include_directories(
        ottergon_${PROJECT_NAME}
        PUBLIC
)
