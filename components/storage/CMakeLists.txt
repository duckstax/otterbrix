project(storage CXX)

list(APPEND ${PROJECT_NAME}_HEADERS
        tuple.hpp

)

list(APPEND ${PROJECT_NAME}_SOURCES
        tuple.cpp
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
