project(collection)

set(${PROJECT_NAME}_HEADERS
        collection.hpp
        result.hpp
        sort.hpp
)

set(${PROJECT_NAME}_SOURCES
        planner/transaction_context.cpp

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
        operators/scan/scan.cpp
        operators/scan/full_scan.cpp
        operators/scan/index_scan.cpp
        operators/scan/primary_key_scan.cpp
        operators/scan/transfer_scan.cpp
        operators/merge/operator_merge.cpp
        operators/merge/operator_and.cpp
        operators/merge/operator_or.cpp
        operators/merge/operator_not.cpp
        operators/predicates/limit.cpp
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

add_library(rocketjoe_${PROJECT_NAME}
        ${${PROJECT_NAME}_HEADERS}
        ${${PROJECT_NAME}_SOURCES}
)


add_library(rocketjoe::${PROJECT_NAME} ALIAS rocketjoe_${PROJECT_NAME})

set_property(TARGET rocketjoe_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        rocketjoe_${PROJECT_NAME} PUBLIC
        rocketjoe::document
        rocketjoe::cursor
        rocketjoe::session
        rocketjoe::index
        CONAN_PKG::spdlog
        CONAN_PKG::abseil
        CONAN_PKG::boost
)

target_include_directories(rocketjoe_${PROJECT_NAME} PUBLIC
)

if (DEV_MODE)
    add_definitions(-DDEV_MODE)
    add_subdirectory(tests)
endif ()
