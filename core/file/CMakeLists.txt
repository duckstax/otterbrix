project(file)

set(header_${PROJECT_NAME}
        file.hpp
        )

set(source_${PROJECT_NAME}
        file.cpp
        )

add_library(ottergon_${PROJECT_NAME}
        ${header_${PROJECT_NAME}}
        ${source_${PROJECT_NAME}}
        )


add_library(ottergon::${PROJECT_NAME} ALIAS ottergon_${PROJECT_NAME})

set_property(TARGET ottergon_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        ottergon_${PROJECT_NAME} PUBLIC
        ${Boost_LIBRARIES}
)

target_include_directories(
        ottergon_${PROJECT_NAME}
        PUBLIC
)
