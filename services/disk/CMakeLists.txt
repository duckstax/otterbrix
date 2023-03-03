project(disk)

set(${PROJECT_NAME}_HEADERS
        command.hpp
        disk.hpp
        agent_disk.hpp
        index_agent_disk.hpp
        manager_disk.hpp
        metadata.hpp
        route.hpp
        result.hpp
)

set(${PROJECT_NAME}_SOURCES
        command.cpp
        disk.cpp
        agent_disk.cpp
        index_agent_disk.cpp
        manager_disk.cpp
        metadata.cpp
        result.cpp
)

add_library(rocketjoe_${PROJECT_NAME}
    ${${PROJECT_NAME}_HEADERS}
    ${${PROJECT_NAME}_SOURCES}
)

add_library(rocketjoe::${PROJECT_NAME} ALIAS rocketjoe_${PROJECT_NAME})

set_property(TARGET rocketjoe_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
    rocketjoe_${PROJECT_NAME} PUBLIC
    rocketjoe::file
    rocketjoe::document
    CONAN_PKG::spdlog
    CONAN_PKG::rocksdb
    CONAN_PKG::boost
)

target_include_directories(rocketjoe_${PROJECT_NAME} PUBLIC
)

if (DEV_MODE)
    add_subdirectory(tests)
endif()
