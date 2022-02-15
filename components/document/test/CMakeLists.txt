project(test_document_base)

set( ${PROJECT_NAME}_SOURCES
    test_api.cpp
    test_encoder.cpp
    test_mutable.cpp
    test_shared_keys.cpp
    test_support.cpp
    test_value.cpp
    test_document.cpp
)

add_executable(${PROJECT_NAME} main.cpp  ${${PROJECT_NAME}_SOURCES})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        rocketjoe::document
        pthread
        ${CONAN_LIBS}
)

file(COPY test_files/small-test.json DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/test)
file(COPY test_files/big-test.json DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/test)

include(CTest)
include(Catch)
catch_discover_tests(${PROJECT_NAME})
