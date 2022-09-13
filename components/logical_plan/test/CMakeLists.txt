project(test_logical_plan)

add_definitions(-DDEV_MODE)

set(${PROJECT_NAME}_SOURCES

)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        ${CONAN_LIBS}
        rocketjoe::document
        rocketjoe::logical_plan
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
