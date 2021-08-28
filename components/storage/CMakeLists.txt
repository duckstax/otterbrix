project(storage_base CXX)

list(APPEND ${PROJECT_NAME}_HEADERS
        collection.hpp
        database.hpp
        document.hpp
        wrapper_client.hpp
        wrapper_collection.hpp
        wrapper_database.hpp
        wrapper_document.hpp
)

list(APPEND ${PROJECT_NAME}_SOURCES
        collection.cpp
        database.cpp
        document.cpp
        wrapper_client.cpp
        wrapper_collection.cpp
        wrapper_database.cpp
        wrapper_document.cpp
)


add_library(rocketjoe_${PROJECT_NAME}
            ${${PROJECT_NAME}_HEADERS}
            ${${PROJECT_NAME}_SOURCES}
)


add_library(rocketjoe::${PROJECT_NAME} ALIAS rocketjoe_${PROJECT_NAME})

set_property(TARGET rocketjoe_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        rocketjoe_${PROJECT_NAME} PRIVATE
        ${CONAN_LIBS}
)

target_include_directories(
        rocketjoe_${PROJECT_NAME}
        PUBLIC
        "${PROJECT_SOURCE_DIR}/components/storage/"
)

add_subdirectory(test)


if (DEV_MODE)
    add_subdirectory(test)
endif()
