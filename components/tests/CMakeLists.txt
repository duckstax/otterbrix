project(test_generaty)

list(APPEND ${PROJECT_NAME}_HEADERS
        generaty.hpp
)

list(APPEND ${PROJECT_NAME}_SOURCES
        generaty.cpp
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
)
