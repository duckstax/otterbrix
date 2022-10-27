project(test_session)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES

)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        rocketjoe::session
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
