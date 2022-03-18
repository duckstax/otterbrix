project(test_serialize)

set(${PROJECT_NAME}_SOURCES
        main.cpp
        test_serialize.cpp
        )

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        ${CONAN_LIBS}
        rocketjoe::document
        rocketjoe::serialize
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
