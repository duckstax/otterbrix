project(test_duck_charmer)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES
        test_collection.cpp
        test_save_load.cpp
        )

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        rocketjoe::ottergon
        rocketjoe::test_generaty
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
