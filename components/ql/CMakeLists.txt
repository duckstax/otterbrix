project(ql CXX)

set(SOURCE_${PROJECT_NAME}
        aggregate/operator.cpp
        ql_param_statement.cpp
        aggregate.cpp
        aggregate/group.cpp
        aggregate/limit.cpp
        aggregate/match.cpp
        aggregate/sort.cpp
        join/join.cpp
        statements/create_collection.cpp
        statements/create_database.cpp
        statements/delete_many.cpp
        statements/delete_one.cpp
        statements/drop_collection.cpp
        statements/drop_database.cpp
        statements/insert_many.cpp
        statements/insert_one.cpp
        statements/update_many.cpp
        statements/update_one.cpp
)

add_library(otterbrix_${PROJECT_NAME}
        ${HEADER_${PROJECT_NAME}}
        ${SOURCE_${PROJECT_NAME}}
)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PUBLIC
        otterbrix::document
        otterbrix::types
        otterbrix::expressions
        Boost::boost
        magic_enum::magic_enum
)

target_include_directories(
        otterbrix_${PROJECT_NAME}
        PUBLIC
)

if (DEV_MODE)
    add_subdirectory(test)
endif ()
