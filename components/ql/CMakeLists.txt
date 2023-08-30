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

add_library(rocketjoe_${PROJECT_NAME}
        ${HEADER_${PROJECT_NAME}}
        ${SOURCE_${PROJECT_NAME}}
)

add_library(rocketjoe::${PROJECT_NAME} ALIAS rocketjoe_${PROJECT_NAME})

set_property(TARGET rocketjoe_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        rocketjoe_${PROJECT_NAME} PUBLIC
        rocketjoe::document
        rocketjoe::expressions
        CONAN_PKG::boost
)

target_include_directories(
        rocketjoe_${PROJECT_NAME}
        PUBLIC
)

if (DEV_MODE)
    add_subdirectory(test)
endif ()
