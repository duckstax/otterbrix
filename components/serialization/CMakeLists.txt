project(serialization)

set(${PROJECT_NAME}_SOURCES
        stream/byte.cpp
        stream/json.cpp
        serialization.cpp
)


add_library(rocketjoe_${PROJECT_NAME}
            ${${PROJECT_NAME}_SOURCES}
)


add_library(rocketjoe::${PROJECT_NAME} ALIAS rocketjoe_${PROJECT_NAME})

set_property(TARGET rocketjoe_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        rocketjoe_${PROJECT_NAME} PUBLIC
        CONAN_PKG::boost
        json
        dl
)

target_include_directories(
        rocketjoe_${PROJECT_NAME}
        PUBLIC
)

if (DEV_MODE)
  add_subdirectory(test)
endif()
