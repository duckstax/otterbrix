project(wal)

set( ${PROJECT_NAME}_HEADERS
)

set(${PROJECT_NAME}_SOURCES
        dto.cpp
        record.cpp
        manager_wal_replicate.cpp
        wal.cpp
)

add_library(ottergon_${PROJECT_NAME}
            ${${PROJECT_NAME}_HEADERS}
            ${${PROJECT_NAME}_SOURCES}
)

add_library(ottergon::${PROJECT_NAME} ALIAS ottergon_${PROJECT_NAME})

set_property(TARGET ottergon_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        ottergon_${PROJECT_NAME} PUBLIC
        ottergon::log
        ottergon::ql
        ottergon::locks
        ottergon::file

        CONAN_PKG::spdlog
        CONAN_PKG::crc32c
        CONAN_PKG::actor-zeta
        CONAN_PKG::boost

)

target_include_directories(
        ottergon_${PROJECT_NAME}
        PRIVATE
        PUBLIC
)

if (DEV_MODE)
    add_definitions(-DDEV_MODE)
    add_subdirectory(tests)
endif()
