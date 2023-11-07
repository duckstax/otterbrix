project(statistic)

set(${PROJECT_NAME}_SOURCES
        statistic.cpp
)

add_library(ottergon_${PROJECT_NAME}
        ${${PROJECT_NAME}_SOURCES}
)

add_library(ottergon::${PROJECT_NAME} ALIAS ottergon_${PROJECT_NAME})

set_property(TARGET ottergon_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        ottergon_${PROJECT_NAME} PUBLIC
        ${COMMON_COMPONENTS}
)

target_include_directories(ottergon_${PROJECT_NAME} PUBLIC session.hpp)

if (DEV_MODE)
    #add_subdirectory(tests)
endif()
