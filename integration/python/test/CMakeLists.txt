project(test_duck_charmer)

set( ${PROJECT_NAME}_SOURCES
        test_collection.cpp
        test_save_load.cpp
        test_convert.cpp
)

add_executable(${PROJECT_NAME} main.cpp ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        ${CONAN_LIBS}
        rocketjoe::ottergon
        rocketjoe::test_generaty
)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
