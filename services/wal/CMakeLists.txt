project(wal)

set( ${PROJECT_NAME}_HEADERS
)

set(${PROJECT_NAME}_SOURCES
        dto.cpp
        record.cpp
        manager_wal_replicate.cpp
        wal.cpp
)

add_library(otterbrix_${PROJECT_NAME}
            ${${PROJECT_NAME}_HEADERS}
            ${${PROJECT_NAME}_SOURCES}
)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PRIVATE
        otterbrix::log
        otterbrix::ql
        otterbrix::locks
        otterbrix::file

        CONAN_PKG::spdlog
        CONAN_PKG::crc32c
        CONAN_PKG::actor-zeta
        CONAN_PKG::boost

)

target_include_directories(
        otterbrix_${PROJECT_NAME}
        PRIVATE
        PUBLIC
)

if (DEV_MODE)
    add_definitions(-DDEV_MODE)
    add_subdirectory(tests)
endif()
