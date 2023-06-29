project(file)

set(header_${PROJECT_NAME}
        file.hpp
        )

set(source_${PROJECT_NAME}
        file.cpp
        )

add_library(rocketjoe_${PROJECT_NAME}
        ${header_${PROJECT_NAME}}
        ${source_${PROJECT_NAME}}
        )


add_library(rocketjoe::${PROJECT_NAME} ALIAS rocketjoe_${PROJECT_NAME})

set_property(TARGET rocketjoe_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        rocketjoe_${PROJECT_NAME} PUBLIC
        ${Boost_LIBRARIES}
)

target_include_directories(
        rocketjoe_${PROJECT_NAME}
        PUBLIC
)
