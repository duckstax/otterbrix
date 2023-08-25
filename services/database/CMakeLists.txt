project(database)

set(${PROJECT_NAME}_HEADERS
        database.hpp
)

set(${PROJECT_NAME}_SOURCES
        database.cpp
)

add_library(ottergon_${PROJECT_NAME}
        ${${PROJECT_NAME}_HEADERS}
        ${${PROJECT_NAME}_SOURCES}
)


add_library(ottergon::${PROJECT_NAME} ALIAS ottergon_${PROJECT_NAME})

set_property(TARGET ottergon_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        ottergon_${PROJECT_NAME} PRIVATE
        ottergon::document
        ottergon::cursor
        ottergon::session
        ottergon::locks
        ottergon::collection
        CONAN_PKG::spdlog
)

target_include_directories(ottergon_${PROJECT_NAME} PUBLIC
)

#if (DEV_MODE)
#    add_definitions(-DDEV_MODE)
#    add_subdirectory(tests)
#endif ()
