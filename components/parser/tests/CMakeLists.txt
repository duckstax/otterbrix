project(test_parser)

set( ${PROJECT_NAME}_SOURCES
    test_query.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        rocketjoe::log
        rocketjoe::storage
        rocketjoe::document
        rocketjoe::parser
        ${CONAN_LIBS}
)

target_include_directories(${PROJECT_NAME} PUBLIC
    ..
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
