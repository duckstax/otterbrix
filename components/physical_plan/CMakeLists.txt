project(physical_plan)


set(${PROJECT_NAME}_SOURCES
        collection/operators/aggregate/operator_aggregate.cpp
        collection/operators/aggregate/operator_count.cpp
        collection/operators/aggregate/operator_min.cpp
        collection/operators/aggregate/operator_max.cpp
        collection/operators/aggregate/operator_sum.cpp
        collection/operators/aggregate/operator_avg.cpp

        collection/operators/get/operator_get.cpp
        collection/operators/get/simple_value.cpp

        collection/operators/merge/operator_merge.cpp
        collection/operators/merge/operator_and.cpp
        collection/operators/merge/operator_or.cpp
        collection/operators/merge/operator_not.cpp

        collection/operators/predicates/predicate.cpp
        collection/operators/predicates/simple_predicate.cpp

        collection/operators/scan/full_scan.cpp
        collection/operators/scan/index_scan.cpp
        collection/operators/scan/primary_key_scan.cpp
        collection/operators/scan/transfer_scan.cpp

        collection/operators/sort/sort.cpp

        collection/operators/operator_data.cpp
        collection/operators/operator_write_data.cpp
        collection/operators/operator.cpp
        collection/operators/operator_add_index.cpp
        collection/operators/operator_drop_index.cpp
        collection/operators/operator_empty.cpp
        collection/operators/operator_insert.cpp
        collection/operators/operator_delete.cpp
        collection/operators/operator_update.cpp
        collection/operators/operator_match.cpp
        collection/operators/operator_group.cpp
        collection/operators/operator_sort.cpp
        collection/operators/operator_join.cpp
        collection/operators/aggregation.cpp
        collection/operators/operator_raw_data.cpp
)

add_library(otterbrix_${PROJECT_NAME}
        ${${PROJECT_NAME}_HEADERS}
        ${${PROJECT_NAME}_SOURCES}
)


add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PUBLIC
        otterbrix::document
        otterbrix::types
        otterbrix::cursor
        otterbrix::session
        otterbrix::context
        otterbrix::index
        otterbrix::logical_plan
        spdlog::spdlog
        abseil::abseil
        Boost::boost
)

target_include_directories(otterbrix_${PROJECT_NAME} PUBLIC
)

if (DEV_MODE)
    add_definitions(-DDEV_MODE)
    add_subdirectory(tests)
endif ()
