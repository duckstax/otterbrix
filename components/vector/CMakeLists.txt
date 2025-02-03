project(vector)

set(CMAKE_CXX_STANDARD 17)

set(SOURCE_${PROJECT_NAME}
        vector.cpp
        vector_buffer.cpp
        indexing_vector.cpp
        vector_operations.cpp
        data_chunk.cpp
        validation.cpp
)
include_directories(${CMAKE_SOURCE_DIR})

add_library(otterbrix_${PROJECT_NAME}
        ${SOURCE_${PROJECT_NAME}}
)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PUBLIC
        otterbrix::types
        otterbrix::string_heap
)

if (DEV_MODE)
    add_subdirectory(tests)
endif ()