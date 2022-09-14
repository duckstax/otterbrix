project(collection)

set(${PROJECT_NAME}_HEADERS
        collection.hpp
        result.hpp
        sort.hpp
)

set(${PROJECT_NAME}_SOURCES
        planner/transaction_context.cpp

        operators/operator_data.cpp
        operators/operator.cpp
        operators/operator_insert.cpp
        operators/operator_delete.cpp
        operators/operator_update.cpp
        operators/full_scan.cpp
        operators/aggregation.cpp
        operators/predicates/limit.cpp
        operators/predicates/predicate.cpp
        operators/predicates/simple_predicate.cpp

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
        rocketjoe_${PROJECT_NAME} PRIVATE
        ${CONAN_LIBS}
        rocketjoe::document
        rocketjoe::cursor
        rocketjoe::session
        rocketjoe::serialize
        rocketjoe::index
)

target_include_directories(rocketjoe_${PROJECT_NAME} PUBLIC
        )

if (DEV_MODE)
    add_definitions(-DDEV_MODE)
    add_subdirectory(tests)
endif ()
