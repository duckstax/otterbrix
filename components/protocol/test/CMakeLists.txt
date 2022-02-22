project(test_protocol)

add_definitions(-DDEV_MODE)

set( ${PROJECT_NAME}_SOURCES

)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        ${CONAN_LIBS}
        rocketjoe::protocol
)

#target_include_directories(
#        ${PROJECT_NAME}
#        PUBLIC
#        ..
#)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
