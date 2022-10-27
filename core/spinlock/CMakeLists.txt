project(locks)

set( header_${PROJECT_NAME}
        spinlock.hpp
)

set( source_${PROJECT_NAME}
        spinlock.cpp
)

add_library(rocketjoe_${PROJECT_NAME}
            ${header_${PROJECT_NAME}}
            ${source_${PROJECT_NAME}}
)


add_library(rocketjoe::${PROJECT_NAME} ALIAS rocketjoe_${PROJECT_NAME})

set_property(TARGET rocketjoe_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        rocketjoe_${PROJECT_NAME} PRIVATE
)

target_include_directories(
        rocketjoe_${PROJECT_NAME}
        PUBLIC
)

if(DEV_MODE)
    #add_subdirectory(test)
endif()

