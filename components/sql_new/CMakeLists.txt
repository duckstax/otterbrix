project(sql_new CXX)

set(SOURCE_${PROJECT_NAME}
    parser/gram.cpp
    parser/pg_functions.cpp
    parser/pg_std_list.cpp
    parser/scan.cpp
    parser/scansup.cpp
    parser/keyword.cpp
    parser/parser.cpp
    parser/expr_location.cpp
    parser/guc.cpp

    parser/nodes/value.cpp
    parser/nodes/makefuncs.cpp
    parser/nodes/nodes.cpp

    transformer/utils.cpp
    transformer/transformer.cpp

    transformer/expressions/parsed_expression.cpp
    transformer/expressions/transform/transform_expression.cpp
    transformer/expressions/transform/transform_columnref.cpp
    transformer/expressions/transform/transform_value.cpp
    transformer/expressions/transform/transform_bool_expr.cpp
    transformer/expressions/transform/transform_case_expr.cpp
    transformer/expressions/transform/transform_coalesce.cpp
    transformer/expressions/transform/transform_null_test.cpp
    transformer/expressions/transform/transform_paramref.cpp
    transformer/expressions/transform/transform_array_indirection.cpp
    transformer/expressions/transform/transform_boolean_test.cpp

    transformer/statements/transform/transform_statement.cpp
    transformer/statements/transform/transform_create_statement.cpp
    transformer/statements/transform/transform_select_statement.cpp
)

include_directories(${CMAKE_SOURCE_DIR})

add_library(otterbrix_${PROJECT_NAME} ${SOURCE_${PROJECT_NAME}})
add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
    otterbrix_${PROJECT_NAME} PUBLIC
    otterbrix::document
    otterbrix::expressions
    otterbrix::ql
)

target_include_directories(
    otterbrix_${PROJECT_NAME}
    PUBLIC
)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -w")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-all")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-all")

if (DEV_MODE)
    add_subdirectory(test)
endif ()
