project(dispatcher)

set( ${PROJECT_NAME}_HEADERS
    dispatcher.hpp
)

set(${PROJECT_NAME}_SOURCES
    dispatcher.cpp
)

add_library(rocketjoe_${PROJECT_NAME}
            ${${PROJECT_NAME}_HEADERS}
            ${${PROJECT_NAME}_SOURCES}
)

add_library(rocketjoe::${PROJECT_NAME} ALIAS rocketjoe_${PROJECT_NAME})

set_property(TARGET rocketjoe_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        rocketjoe_${PROJECT_NAME} PRIVATE
        ${CONAN_LIBS}
        rocketjoe::log
        rocketjoe::cursor
        rocketjoe::session
)

target_include_directories(rocketjoe_${PROJECT_NAME} PUBLIC
        dispatcher.hpp
)

if (DEV_MODE)
#    add_subdirectory(tests)
endif()
