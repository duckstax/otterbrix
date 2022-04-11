project(test_oid)

set( ${PROJECT_NAME}_SOURCES
        test_hex.cpp
        test_oid.cpp
        test_oid_generator.cpp
)

add_executable(${PROJECT_NAME} main.cpp  ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        ${CONAN_LIBS}
        rocketjoe::oid
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
