project(cursor)

set( ${PROJECT_NAME}_HEADERS
        cursor.hpp
)

set(${PROJECT_NAME}_SOURCES
        cursor.cpp
)

add_library(otterbrix_${PROJECT_NAME}
        ${${PROJECT_NAME}_HEADERS}
        ${${PROJECT_NAME}_SOURCES}
)

add_library(otterbrix::${PROJECT_NAME} ALIAS otterbrix_${PROJECT_NAME})

set_property(TARGET otterbrix_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        otterbrix_${PROJECT_NAME} PRIVATE
        actor-zeta::actor-zeta
        Boost::boost
        absl::int128
)

target_include_directories(otterbrix_${PROJECT_NAME}
        PUBLIC
)

if (DEV_MODE)
    add_subdirectory(tests)
endif()
