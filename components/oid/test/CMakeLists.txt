project(test_oid)

set( ${PROJECT_NAME}_SOURCES
        test_oid.cpp
)

add_executable(${PROJECT_NAME} main.cpp  ${${PROJECT_NAME}_SOURCES})

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
