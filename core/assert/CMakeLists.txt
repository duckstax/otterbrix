project(assert)

set(source_${PROJECT_NAME}
        assert.cpp
        trace_full_exception.cpp
        )

add_library(ottergon_${PROJECT_NAME}
        ${source_${PROJECT_NAME}}
        )


add_library(ottergon::${PROJECT_NAME} ALIAS ottergon_${PROJECT_NAME})

set_property(TARGET ottergon_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        ottergon_${PROJECT_NAME} PRIVATE
        rocketjoe::log
)

target_include_directories(
        ottergon_${PROJECT_NAME}
        PUBLIC
)

if (DEV_MODE)
    add_subdirectory(tests)
endif ()

