project(collection)

set(${PROJECT_NAME}_HEADERS
        collection.hpp
        result.hpp
        sort.hpp
)

set(${PROJECT_NAME}_SOURCES
        planner/create_plan.cpp
        planner/impl/create_plan_aggregate.cpp
        planner/impl/create_plan_delete.cpp
        planner/impl/create_plan_group.cpp
        planner/impl/create_plan_insert.cpp
        planner/impl/create_plan_match.cpp
        planner/impl/create_plan_sort.cpp
        planner/impl/create_plan_update.cpp

        operators/operator_data.cpp
        operators/operator_write_data.cpp
        operators/operator.cpp
        operators/operator_empty.cpp
        operators/operator_insert.cpp
        operators/operator_delete.cpp
        operators/operator_update.cpp
        operators/operator_group.cpp
        operators/operator_sort.cpp
        operators/aggregation.cpp
        operators/scan/full_scan.cpp
        operators/scan/index_scan.cpp
        operators/scan/primary_key_scan.cpp
        operators/scan/transfer_scan.cpp
        operators/merge/operator_merge.cpp
        operators/merge/operator_and.cpp
        operators/merge/operator_or.cpp
        operators/merge/operator_not.cpp
        operators/predicates/predicate.cpp
        operators/predicates/simple_predicate.cpp
        operators/aggregate/operator_aggregate.cpp
        operators/aggregate/operator_count.cpp
        operators/aggregate/operator_min.cpp
        operators/aggregate/operator_max.cpp
        operators/aggregate/operator_sum.cpp
        operators/aggregate/operator_avg.cpp
        operators/get/operator_get.cpp
        operators/get/simple_value.cpp

        collection.cpp
        create_index.cpp
        result.cpp
        sort.cpp
)

add_library(ottergon_${PROJECT_NAME}
        ${${PROJECT_NAME}_HEADERS}
        ${${PROJECT_NAME}_SOURCES}
)


add_library(ottergon::${PROJECT_NAME} ALIAS ottergon_${PROJECT_NAME})

set_property(TARGET ottergon_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        ottergon_${PROJECT_NAME} PUBLIC
        ottergon::document
        ottergon::cursor
        ottergon::session
        ottergon::index
        ottergon::logical_plan
        ottergon::pipeline
        CONAN_PKG::spdlog
        CONAN_PKG::abseil
        CONAN_PKG::boost
)

target_include_directories(ottergon_${PROJECT_NAME} PUBLIC
)

if (DEV_MODE)
    add_definitions(-DDEV_MODE)
    add_subdirectory(tests)
endif ()
