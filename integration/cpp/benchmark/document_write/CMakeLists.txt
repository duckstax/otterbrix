set(project benchmark_document_write)

cmake_policy(SET CMP0048 NEW)
PROJECT(${project} VERSION "${CMAKE_PROJECT_VERSION}" LANGUAGES CXX)

message(STATUS "PROJECT_NAME = ${PROJECT_NAME}")

add_executable(${PROJECT_NAME} main.cpp)

target_link_libraries(
        ${PROJECT_NAME} PRIVATE
        CONAN_PKG::benchmark
        ottergon::document
)

file(COPY start-perf DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
