project(locks)

set(header_${PROJECT_NAME}
        spinlock.hpp
        )

set(source_${PROJECT_NAME}
        spinlock.cpp
        )

add_library(ottergon_${PROJECT_NAME}
        ${header_${PROJECT_NAME}}
        ${source_${PROJECT_NAME}}
        )


add_library(ottergon::${PROJECT_NAME} ALIAS ottergon_${PROJECT_NAME})

set_property(TARGET ottergon_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        ottergon_${PROJECT_NAME} PRIVATE
        ${Boost_LIBRARIES}
)

target_include_directories(
        ottergon_${PROJECT_NAME}
        PUBLIC
)

if (DEV_MODE)
    #add_subdirectory(test)
endif ()

