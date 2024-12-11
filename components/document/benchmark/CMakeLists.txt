set(project benchmark_document)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g -fno-omit-frame-pointer ")

PROJECT(${project} VERSION "${CMAKE_PROJECT_VERSION}" LANGUAGES CXX)

set(${PROJECT_NAME}_SOURCES
        main.cpp
        )

message(STATUS "PROJECT_NAME = ${PROJECT_NAME}")

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SOURCES} ${${PROJECT_NAME}_HEADERS})

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        otterbrix::document
        otterbrix::test_generaty
        benchmark::benchmark
)

file(COPY start-perf DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
