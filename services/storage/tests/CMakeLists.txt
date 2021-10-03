project(test_storage)

set( ${PROJECT_NAME}_SOURCES
    test_query.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        rocketjoe::storage
)
