project(dataframe)

set(${PROJECT_NAME}_SOURCES
        detail/bits.cpp
        detail/bitmask.cpp
        bitmask.cpp
        traits.cpp
)


add_library(ottergon_${PROJECT_NAME}
        ${${PROJECT_NAME}_SOURCES}
)


add_library(ottergon::${PROJECT_NAME} ALIAS ottergon_${PROJECT_NAME})

set_property(TARGET ottergon_${PROJECT_NAME} PROPERTY EXPORT_NAME ${PROJECT_NAME})

target_link_libraries(
        ottergon_${PROJECT_NAME} PUBLIC
        CONAN_PKG::boost
        ottergon::assert
        dl
)

target_include_directories(
        ottergon_${PROJECT_NAME}
        PUBLIC
)

if (DEV_MODE)
    add_subdirectory(tests)
endif ()
