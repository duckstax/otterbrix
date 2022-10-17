project(wal)

set( ${PROJECT_NAME}_HEADERS
)

set(${PROJECT_NAME}_SOURCES
        dto.cpp
        record.cpp
        manager_wal_replicate.cpp
        wal.cpp
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
        rocketjoe::log
        rocketjoe::ql
        rocketjoe::locks
        rocketjoe::file
)

target_include_directories(
        rocketjoe_${PROJECT_NAME}
        PRIVATE
        dto.hpp
        PUBLIC
        record.hpp
        wal.hpp
        manager_wal_replicate.hpp
)

if (DEV_MODE)
    add_definitions(-DDEV_MODE)
    add_subdirectory(tests)
endif()
