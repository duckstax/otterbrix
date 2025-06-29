project(catalog CXX)

set(SOURCE_${PROJECT_NAME}
        transaction/schema_diff.cpp
        transaction/metadata_diff.cpp

        transaction/metadata_transaction.cpp
        transaction/transaction_scope.cpp
        transaction/transaction_list.cpp

        table_id.cpp
        schema.cpp
        table_metadata.cpp
        namespace_storage.cpp
        catalog.cpp
)

add_library(otterbrix_${PROJECT_NAME}
        ${SOURCE_${PROJECT_NAME}}
)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PRIVATE
        otterbrix::types
        magic_enum::magic_enum
        Boost::boost # temporary
)

target_include_directories(
        otterbrix_${PROJECT_NAME}
        PUBLIC
)

#if (DEV_MODE)
add_definitions(-DDEV_MODE)
add_subdirectory(tests)
#endif ()